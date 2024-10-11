import typing
from pathlib import Path
from numpy import repeat
from polars import DataFrame, Series, String, Float64
from polars import concat, from_dicts

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

from Code.Pipeline.account_derivation import create_derived_ledger_entries, verify_account_correspondence

from Code.Data.account_data import ledger_columns, Account, DerivedAccount, LedgerConfiguration, AccountMapping, LedgerImport

from Code.source_database import SourceDataBase
from Code.derived_database import DerivedDataBase
from Code.database import JsonDataBase
from Code.Utils.json_serializer import json_serializer

def make_account_data_table(account : Account) -> DataFrame :
    account_data = account.transactions[["date", "description", "delta"]]
    balance_list = []
    current_balance = account.start_value
    for transaction in account.transactions.rows() :
        current_balance += transaction[2]
        balance_list.append(round(current_balance, 2))
    balance_frame = DataFrame(Series("Balance", balance_list))
    return concat([account_data, balance_frame], how="horizontal")

def ledger_entries_from_derived(account_derivations : typing.List[DerivedAccount], source_accounts : SourceDataBase) -> DataFrame :
    try :
        new_ledger_entries = create_derived_ledger_entries(account_derivations, source_accounts)
    except Exception as e :
        logger.info(f"Hit exception ({e}) when trying to derive!")
    return new_ledger_entries

class UnaccountedTransactionFrame :

    unaccounted_transaction_object_name = "UnaccountedTransactions"
    empty_frame = DataFrame(schema={
        "date" : String,
        "description" : String,
        "delta" : Float64,
        "account" : String
    })

    def __init__(self, config_db : JsonDataBase) :
        self.__configuration_data = config_db

    def clear(self) -> None :
        self.update(UnaccountedTransactionFrame.empty_frame)

    def retrieve(self) -> DataFrame :
        object_name = UnaccountedTransactionFrame.unaccounted_transaction_object_name
        if self.__configuration_data.is_stored(object_name) :
            return from_dicts(self.__configuration_data.retrieve(object_name)["unaccounted"])
        else :
            self.clear()
            return UnaccountedTransactionFrame.empty_frame

    def update(self, unaccounted_transactions : DataFrame) -> None :
        object_name = UnaccountedTransactionFrame.unaccounted_transaction_object_name
        self.__configuration_data.update(object_name, {"unaccounted" : unaccounted_transactions.to_dicts()})
    
def verify_and_concat_ledger_entries(current_entries : DataFrame, new_ledger_entries : DataFrame) -> DataFrame :
        assert new_ledger_entries.columns == ledger_columns, "Incompatible columns detected!"
        new_ids = frozenset(concat([new_ledger_entries["from_transaction_id"], new_ledger_entries["to_transaction_id"]]))
        if len(new_ids) > 0 :
            current_accounted_ids = frozenset(concat([current_entries["from_transaction_id"], current_entries["to_transaction_id"]]))
            assert new_ids.isdisjoint(current_accounted_ids), f"Duplicate unique hashes already existing in ledger:\n{list(new_ids - current_accounted_ids)}\n, likely double matched!"
            return concat([current_entries, new_ledger_entries])
        return current_entries

def get_ledger_configuration(dataroot_path : Path) -> LedgerConfiguration :
    ledger_config_path = dataroot_path / "LedgerConfiguration.json"
    try :
        read_object = json_serializer.read_from_file(ledger_config_path, LedgerConfiguration)
        assert isinstance(read_object, LedgerConfiguration), f"Failed to deserialize LedgerConfiguration! {ledger_config_path}"
        return read_object
    except Exception as e :
        logger.info(f"Failed to read ledger configuration from {ledger_config_path}! {e}")
    return LedgerConfiguration()

class LedgerDataBase :

    def __init__(self, root_path : Path, ledger_import : LedgerImport, account_mapping : AccountMapping) :
        ledger_output_path = root_path / ledger_import.ledger_name
        name = ledger_import.ledger_name
        self.__config_db = JsonDataBase(ledger_output_path, "Config")

        try :
            account_data_path = root_path / ledger_import.source_account_folder
            source_db = SourceDataBase(self.__config_db, ledger_output_path, ledger_import.raw_accounts, account_data_path)
            logger.info(f"Source database created for {name}")
            self.__source_db = source_db
        except Exception as e :
            logger.error(f"Failed to build source database for ledger {name}! {e}")

        try :
            derived_db = DerivedDataBase(self.__config_db, self.__source_db, ledger_output_path, account_mapping.derived_accounts)
            logger.info(f"Source database created for {name}")
            self.__derived_db = derived_db
        except Exception as e :
            logger.error(f"Failed to build derived database for ledger {name}! {e}")

        mapped_account_cache = {}
        for mapping in account_mapping.internal_transactions :
            if mapping.from_account not in mapped_account_cache :
                mapped_account_cache[mapping.from_account] = self.__source_db.get_account(mapping.from_account)
            if mapping.to_account not in mapped_account_cache :
                mapped_account_cache[mapping.to_account] = self.__source_db.get_account(mapping.to_account)

        logger.info("Start interaccount verfication...")

        derived_ledger_entries = ledger_entries_from_derived(account_mapping.derived_accounts, self.__source_db)
        for mapping in account_mapping.internal_transactions :
            #internal transaction mappings
            if mapping.from_account != mapping.to_account :
                logger.info(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
                new_ledger_entries = verify_account_correspondence(mapped_account_cache, mapping)
                derived_ledger_entries = verify_and_concat_ledger_entries(derived_ledger_entries, new_ledger_entries)
            else :
                logger.error(f"Transactions to same account {mapping.from_account}?")
        self.__config_db.update("LedgerEntries", {"entries" : derived_ledger_entries.to_dicts()})
        accounted_transaction_ids = DataFrame(Series("ID", list(concat([derived_ledger_entries["from_transaction_id"], derived_ledger_entries["to_transaction_id"]]))))

        self.__unaccouted_transactions = UnaccountedTransactionFrame(self.__config_db)
        unaccounted_transactions_data_frame_list = []
        for account_data in self.get_source_accounts() :
            unaccounted_dataframe = (account_data.transactions
                .join(accounted_transaction_ids, "ID", "anti")
                .select(["date", "description", "delta"]))
            account_column = Series("account", repeat(account_data.name, unaccounted_dataframe.height))
            unaccounted_dataframe = unaccounted_dataframe.insert_column(unaccounted_dataframe.width, account_column)
            unaccounted_transactions_data_frame_list.append(unaccounted_dataframe)
        if len(unaccounted_transactions_data_frame_list) > 0 :
            unaccounted_transactions = concat(unaccounted_transactions_data_frame_list)
            unaccounted_transactions = unaccounted_transactions.insert_column(0, Series("index", range(0, unaccounted_transactions.height)))
            self.__unaccouted_transactions.update(unaccounted_transactions)
        else :
            self.__unaccouted_transactions.clear()

    def account_is_created(self, account_name : str) -> bool :
        return self.__source_db.is_stored(account_name) != self.__derived_db.is_stored(account_name)

    def get_account(self, account_name : str) -> Account :
        if self.__source_db.is_stored(account_name) :
            return self.__source_db.get_account(account_name)
        else :
            assert self.__derived_db.is_stored(account_name), f"Account {account_name} is not in base or derived DBs?"
            return self.__derived_db.get_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__source_db.get_names()
    
    def get_derived_account_names(self) -> typing.List[str] :
        return self.__derived_db.get_names()
    
    def get_source_accounts(self) -> typing.Generator[Account, None, None] :
        for account_name in self.__source_db.get_names() :
            yield self.__source_db.get_account(account_name)

    def get_unaccounted_transaction_table(self) -> DataFrame :
        return self.__unaccouted_transactions.retrieve()

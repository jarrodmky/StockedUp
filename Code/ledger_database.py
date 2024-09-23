import typing
from pathlib import Path
from numpy import repeat
from polars import DataFrame, Series, String, Float64
from polars import concat, from_dicts

from Code.logger import get_logger
logger = get_logger(__name__)

from Code.Pipeline.account_importing import import_ledger_source_accounts
from Code.Pipeline.account_derivation import create_derived_accounts, create_derived_ledger_entries, verify_account_correspondence

from Code.Data.account_data import ledger_columns, Account, DerivedAccount

from Code.database import JsonDataBase
from Code.Utils.json_file import json_read
from Code.Utils.utf8_file import utf8_file

AccountCache = typing.Dict[str, Account]

def make_account_data_table(account : Account) -> DataFrame :
    account_data = account.transactions[["date", "description", "delta"]]
    balance_list = []
    current_balance = account.start_value
    for transaction in account.transactions.rows() :
        current_balance += transaction[2]
        balance_list.append(round(current_balance, 2))
    balance_frame = DataFrame(Series("Balance", balance_list))
    return concat([account_data, balance_frame], how="horizontal")

def get_account_derivations_internal(accountmapping_file : Path) -> typing.List[DerivedAccount] :
    if not accountmapping_file.exists() :
        with utf8_file(accountmapping_file, 'x') as new_mapping_file :
            new_mapping_file.write("{\n")
            new_mapping_file.write("\t\"derived accounts\": [],\n")
            new_mapping_file.write("\t\"internal transactions\": []\n")
            new_mapping_file.write("}")
        return []
    else :
        return json_read(accountmapping_file)["derived accounts"]

def get_account_derivations(dataroot : Path, ledger_name : str) -> typing.List[DerivedAccount] :
    ledger_config = json_read(dataroot.joinpath("LedgerConfiguration.json"))
    for ledger_import in ledger_config.ledgers :
        if ledger_import.name == ledger_name :
            account_mapping_file_path = dataroot / (ledger_import.accounting_file + ".json")
            return get_account_derivations_internal(account_mapping_file_path)
    return []

def import_source_accounts(dataroot_path : Path, ledger_name : str) -> typing.List[Account] :
    ledger_config_path = dataroot_path / "LedgerConfiguration.json"
    imported_accounts = []
    try :
        imported_accounts = import_ledger_source_accounts(ledger_config_path, dataroot_path, ledger_name)
    except Exception as e :
        logger.info(f"Failed to import accounts from {ledger_name}! {e}")
    return imported_accounts

def derive_accounts_from_source(dataroot_path : Path, ledger_name : str, source_account_cache : AccountCache) -> typing.List[Account] :
    try :
        account_derivations = get_account_derivations(dataroot_path, ledger_name)
        imported_accounts = create_derived_accounts(account_derivations, source_account_cache)
    except Exception as e :
        logger.info(f"Hit exception ({e}) when trying to derive!")
    return imported_accounts

def ledger_entries_from_derived(dataroot_path : Path, ledger_name : str, source_account_cache : AccountCache) -> DataFrame :
    try :
        account_derivations = get_account_derivations(dataroot_path, ledger_name)
        new_ledger_entries = create_derived_ledger_entries(account_derivations, source_account_cache)
    except Exception as e :
        logger.info(f"Hit exception ({e}) when trying to derive!")
    return new_ledger_entries

class LedgerEntryFrame :

    ledger_entires_object_name = "LedgerEntries"
    empty_frame = DataFrame(schema={
            "from_account_name" : String,
            "from_transaction_id" : String,
            "to_account_name" : String,
            "to_transaction_id" : String,
            "delta" : Float64
            })

    def __init__(self, ledgerfolder_path : Path) :
        self.__configuration_data = JsonDataBase(ledgerfolder_path, "Config")

    def clear(self) -> None :
        self.update(LedgerEntryFrame.empty_frame)

    def retrieve(self) -> DataFrame :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        if self.__configuration_data.is_stored(object_name) :
            return from_dicts(self.__configuration_data.retrieve(object_name)["entries"], schema=ledger_columns)
        else :
            self.clear()
            return LedgerEntryFrame.empty_frame

    def update(self, ledger_entries : DataFrame) -> None :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        self.__configuration_data.update(object_name, {"entries" : ledger_entries.to_dicts()})

class UnaccountedTransactionFrame :

    unaccounted_transaction_object_name = "UnaccountedTransactions"
    empty_frame = DataFrame(schema={
        "date" : String,
        "description" : String,
        "delta" : Float64,
        "account" : String
    })

    def __init__(self, ledgerfolder_path : Path) :
        self.__configuration_data = JsonDataBase(ledgerfolder_path, "Config")

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

class AccountDatabase :

    def __init__(self, ledgerfile_path : Path, db_name : str, derived_accounts : typing.List[Account]) :
        self.__account_data = JsonDataBase(ledgerfile_path, db_name)
        for derived_account in derived_accounts :
            self.__account_data.update(derived_account.name, derived_account)

    def get_account(self, account_name : str) -> Account :
        assert self.__account_data.is_stored(account_name)
        return self.__account_data.retrieve(account_name)

    def get_account_names(self) -> typing.List[str] :
        return self.__account_data.get_names()

    def has_account(self, account_name : str) -> bool :
        return self.__account_data.is_stored(account_name)
    
def verify_and_concat_ledger_entries(current_entries : DataFrame, new_ledger_entries : DataFrame) -> DataFrame :
        assert new_ledger_entries.columns == ledger_columns, "Incompatible columns detected!"
        new_ids = frozenset(concat([new_ledger_entries["from_transaction_id"], new_ledger_entries["to_transaction_id"]]))
        if len(new_ids) > 0 :
            current_accounted_ids = frozenset(concat([current_entries["from_transaction_id"], current_entries["to_transaction_id"]]))
            assert new_ids.isdisjoint(current_accounted_ids), f"Duplicate unique hashes already existing in ledger:\n{list(new_ids - current_accounted_ids)}\n, likely double matched!"
            return concat([current_entries, new_ledger_entries])
        return current_entries

class LedgerDataBase :

    def __init__(self, root_path : Path, name : str, accounting_filename : str) :
        ledgerfolder_path = root_path / name

        source_accounts = import_source_accounts(root_path, name)
        self.__source_account_data = AccountDatabase(ledgerfolder_path, "BaseAccounts", source_accounts)

        source_account_cache = {}
        for account in source_accounts :
            source_account_cache[account.name] = account

        derived_accounts = derive_accounts_from_source(root_path, name, source_account_cache)
        self.__derived_account_data = AccountDatabase(ledgerfolder_path, "DerivedAccounts", derived_accounts)

        account_mapping_file_path = root_path / (accounting_filename + ".json")
        internal_transactions = []
        if account_mapping_file_path.exists() :
            internal_transactions = json_read(account_mapping_file_path)["internal transactions"]

        mapped_account_cache = {}
        for mapping in internal_transactions :
            if mapping.from_account not in mapped_account_cache :
                mapped_account_cache[mapping.from_account] = self.get_account(mapping.from_account)
            if mapping.to_account not in mapped_account_cache :
                mapped_account_cache[mapping.to_account] = self.get_account(mapping.to_account)

        logger.info("Start interaccount verfication...")

        derived_ledger_entries = ledger_entries_from_derived(root_path, name, source_account_cache)
        for mapping in internal_transactions :
            #internal transaction mappings
            if mapping.from_account != mapping.to_account :
                logger.info(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
                new_ledger_entries = verify_account_correspondence(mapped_account_cache, mapping)
                derived_ledger_entries = verify_and_concat_ledger_entries(derived_ledger_entries, new_ledger_entries)
            else :
                logger.error(f"Transactions to same account {mapping.from_account}?")
        ledger_entries = LedgerEntryFrame(ledgerfolder_path)
        ledger_entries.update(derived_ledger_entries)
        accounted_transaction_ids = DataFrame(Series("ID", list(concat([derived_ledger_entries["from_transaction_id"], derived_ledger_entries["to_transaction_id"]]))))

        self.__unaccouted_transactions = UnaccountedTransactionFrame(ledgerfolder_path)
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
        return self.__source_account_data.has_account(account_name) != self.__derived_account_data.has_account(account_name)

    def get_account(self, account_name : str) -> Account :
        if self.__source_account_data.has_account(account_name) :
            return self.__source_account_data.get_account(account_name)
        else :
            assert self.__derived_account_data.has_account(account_name), f"Account {account_name} is not in base or derived DBs?"
            return self.__derived_account_data.get_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__source_account_data.get_account_names()
    
    def get_derived_account_names(self) -> typing.List[str] :
        return self.__derived_account_data.get_account_names()
    
    def get_source_accounts(self) -> typing.Generator[Account, None, None] :
        for account_name in self.__source_account_data.get_account_names() :
            yield self.__source_account_data.get_account(account_name)

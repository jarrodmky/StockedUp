import typing
from pathlib import Path
from numpy import repeat
from polars import DataFrame, Series, String, Float64
from polars import concat, from_dicts

from Code.logger import get_logger
logger = get_logger(__name__)

from Code.Pipeline.account_importing import import_ledger_source_accounts
from Code.Pipeline.account_derivation import get_account_derivations, create_derived_account, AccountLedgerEntries

from Code.Data.account_data import ledger_columns, Account
from Code.Data.hashing import managed_account_data_hash, UniqueHashCollector

from Code.database import JsonDataBase

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

def import_source_accounts(dataroot_path : Path, ledger_name : str) -> typing.List[Account] :
    ledger_config_path = dataroot_path / "LedgerConfiguration.json"
    imported_accounts = []
    try :
        imported_accounts = import_ledger_source_accounts(ledger_config_path, dataroot_path, ledger_name)
    except Exception as e :
        logger.info(f"Failed to import accounts from {ledger_name}! {e}")
    return imported_accounts

def create_derived_accounts(dataroot_path : Path, ledger_name : str, source_account_cache : AccountCache) -> typing.List[AccountLedgerEntries] :
    derived_accounts = []
    for account_derivation in get_account_derivations(dataroot_path, ledger_name) :
        logger.info(f"Mapping spending account \"{account_derivation.name}\"")
        try :
            account, derived_ledger_entries = create_derived_account(source_account_cache, account_derivation)
            if len(account.transactions) > 0 :
                derived_accounts.append((account, derived_ledger_entries))
                logger.info(f"... account {account_derivation.name} derived!")
            else :
                logger.info(f"... nothing to map for {account_derivation.name}!")
        except Exception as e :
            logger.info(f"... hit exception ({e}) when trying to derive {account_derivation.name}!")
    return derived_accounts

class SourceAccountDatabase :

    def __init__(self, ledgerfile_path : Path, source_accounts : typing.List[Account]) :
        self.__account_data = JsonDataBase(ledgerfile_path, "BaseAccounts")
        for account in source_accounts :
            self.__account_data.update(account.name, account)

    def get_source_account(self, account_name : str) -> Account :
        assert self.__account_data.is_stored(account_name)
        return self.__account_data.retrieve(account_name)

    def get_source_account_names(self) -> typing.List[str] :
        return self.__account_data.get_names()

    def has_source_account(self, account_name : str) -> bool :
        return self.__account_data.is_stored(account_name)

class LedgerEntryFrame :

    ledger_entires_object_name = "LedgerEntries"

    def __init__(self, ledgerfolder_path : Path) :
        self.__configuration_data = JsonDataBase(ledgerfolder_path, "Config")

    def retrieve(self) -> DataFrame :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        if self.__configuration_data.is_stored(object_name) :
            return from_dicts(self.__configuration_data.retrieve(object_name)["entries"], schema=ledger_columns)
        else :
            empty_frame = DataFrame(schema={
                    "from_account_name" : String,
                    "from_transaction_id" : String,
                    "to_account_name" : String,
                    "to_transaction_id" : String,
                    "delta" : Float64
                    })
            self.update(empty_frame)
            return empty_frame

    def update(self, ledger_entries : DataFrame) -> None :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        self.__configuration_data.update(object_name, {"entries" : ledger_entries.to_dicts()})

    def append(self, new_ledger_entries : DataFrame) -> None :
        assert new_ledger_entries.columns == ledger_columns, "Incompatible columns detected!"
        new_ids = frozenset(concat([new_ledger_entries["from_transaction_id"], new_ledger_entries["to_transaction_id"]]))
        if len(new_ids) > 0 :
            current_entries = self.retrieve()
            current_accounted_ids = frozenset(concat([current_entries["from_transaction_id"], current_entries["to_transaction_id"]]))
            assert new_ids.isdisjoint(current_accounted_ids), f"Duplicate unique hashes already existing in ledger:\n{list(new_ids - current_accounted_ids)}\n, likely double matched!"
            self.update(concat([current_entries, new_ledger_entries]))

class DerivedAccountDatabase :

    def __init__(self, ledgerfile_path : Path, derived_accounts : typing.List[Account]) :
        self.__account_data = JsonDataBase(ledgerfile_path, "DerivedAccounts")
        for derived_account in derived_accounts :
            self.__account_data.update(derived_account.name, derived_account)

    def get_derived_account(self, account_name : str) -> Account :
        assert self.__account_data.is_stored(account_name)
        return self.__account_data.retrieve(account_name)

    def get_derived_account_names(self) -> typing.List[str] :
        return self.__account_data.get_names()

    def has_derived_account(self, account_name : str) -> bool :
        return self.__account_data.is_stored(account_name)


class LedgerDataBase :

    def __init__(self, root_path : Path, name : str) :
        self.hash_register = UniqueHashCollector()
        ledgerfolder_path = root_path / name

        source_accounts = import_source_accounts(root_path, name)
        for account in source_accounts :
            account.ID = managed_account_data_hash(self.hash_register, account)
            self.hash_register.register_hash("Account", account.ID, f"Acct={account.name}")
        self.__source_account_data = SourceAccountDatabase(ledgerfolder_path, source_accounts)

        source_account_cache = {}
        for account in source_accounts :
            source_account_cache[account.name] = account

        self.ledger_entries = LedgerEntryFrame(ledgerfolder_path)
        derived_accounts = create_derived_accounts(root_path, name, source_account_cache)
        for derived_account, new_ledger_entries in derived_accounts :
            self.ledger_entries.append(new_ledger_entries)
            derived_account.ID = managed_account_data_hash(self.hash_register, derived_account)
            self.hash_register.register_hash("Account", derived_account.ID, f"Acct={derived_account.name}")
        self.__derived_account_data = DerivedAccountDatabase(ledgerfolder_path, [derived_account for derived_account, _ in derived_accounts])

    def account_is_created(self, account_name : str) -> bool :
        return self.__source_account_data.has_source_account(account_name) != self.__derived_account_data.has_derived_account(account_name)

    def get_account(self, account_name : str) -> Account :
        if self.__source_account_data.has_source_account(account_name) :
            return self.__source_account_data.get_source_account(account_name)
        else :
            assert self.__derived_account_data.has_derived_account(account_name), f"Account {account_name} is not in base or derived DBs?"
            return self.__derived_account_data.get_derived_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__source_account_data.get_source_account_names()
    
    def get_derived_account_names(self) -> typing.List[str] :
        return self.__derived_account_data.get_derived_account_names()
    
    def get_source_accounts(self) -> typing.Generator[Account, None, None] :
        for account_name in self.__source_account_data.get_source_account_names() :
            yield self.__source_account_data.get_source_account(account_name)

    def get_accounted_transactions(self) -> typing.List[str] :
        current_entries = self.ledger_entries.retrieve()
        return list(concat([current_entries["from_transaction_id"], current_entries["to_transaction_id"]]))
        

    def get_unaccounted_transaction_table(self) -> DataFrame :
        empty_frame = DataFrame(schema={
            "date" : String,
            "description" : String,
            "delta" : Float64,
            "account" : String
            })
        
        unaccounted_transactions_data_frame_list = [empty_frame]
        for account_data in self.get_source_accounts() :
            unaccounted_dataframe = (account_data.transactions
                .join(DataFrame(Series("ID", self.get_accounted_transactions())), "ID", "anti")
                .select(["date", "description", "delta"]))
            account_column = Series("account", repeat(account_data.name, unaccounted_dataframe.height))
            unaccounted_dataframe = unaccounted_dataframe.insert_column(unaccounted_dataframe.width, account_column)
            unaccounted_transactions_data_frame_list.append(unaccounted_dataframe)
        unaccounted_transactions = concat(unaccounted_transactions_data_frame_list)
        return unaccounted_transactions.insert_column(0, Series("index", range(0, unaccounted_transactions.height)))

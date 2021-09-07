import pathlib
import typing

from accounting_objects import Transaction, LedgerTransaction, Account, AccountDataTable
from json_file import json_read, json_write
from debug import debug_assert, debug_message
from import_csv_data import transactions_from_csvs

data_path = pathlib.Path("Data")
if not data_path.exists() :
    data_path.mkdir()

AccountList = typing.List[Account]

def load_accounts_from_directory(account_directory : pathlib.Path) -> AccountList :
    account_list = []
    for account_folder_entry in account_directory.iterdir() :
        if account_folder_entry.is_file() :
            account_list.append(json_read(account_folder_entry))
        elif account_folder_entry.is_dir() :
            account_list.extend(AccountManager.__load_accounts(account_folder_entry))
        else :
            debug_message(f"Could not handle directory entry named \"{account_folder_entry}\"")
    return account_list

ManagedAccountDataAndTable = typing.Tuple[Account, AccountDataTable]

def make_managed_account(account_data : Account) -> ManagedAccountDataAndTable :
    return (account_data, AccountDataTable(account_data))

class AccountManager :

    def __init__(self, loaded_base_accounts : AccountList, loaded_derived_accounts : AccountList) :
        self.base_account_lookup : typing.Mapping[str, ManagedAccountDataAndTable] = {}
        for account in loaded_base_accounts :
            self.base_account_lookup[account.name] = make_managed_account(account)

        self.derived_account_lookup : typing.Mapping[str, ManagedAccountDataAndTable] = {}
        for account in loaded_derived_accounts :
            self.derived_account_lookup[account.name] = make_managed_account(account)

    def __get_account_data_pair(self, account_name : str) -> ManagedAccountDataAndTable :
        if account_name in self.base_account_lookup :
            return self.base_account_lookup[account_name]
        elif account_name in self.derived_account_lookup :
            return self.derived_account_lookup[account_name]
        else :
            return None

    def get_account_names(self) -> typing.List[str] :
        derived_account_names = list(self.derived_account_lookup.keys())
        base_account_names = list(self.base_account_lookup.keys())
        return sorted(base_account_names + derived_account_names)

    def get_account_data(self, account_name : str) -> Account :
        data_set = self.__get_account_data_pair(account_name)
        if data_set is not None :
            return data_set[0]
        return None

    def get_account_table(self, account_name : str) -> AccountDataTable :
        data_set = self.__get_account_data_pair(account_name)
        if data_set is not None :
            return data_set[1]
        return None

    def create_account_from_transactions(self, output_filepath : pathlib.Path, transactions : typing.List[Transaction] = [], open_balance : float = 0.0) -> bool :
        new_account = Account(output_filepath.stem, open_balance, transactions)

        with open(output_filepath, 'x') as _ :
            pass

        json_write(output_filepath, new_account)
        self.derived_account_lookup[output_filepath.stem] = make_managed_account(new_account)

    def create_account_from_csv(self, output_filepath : pathlib.Path, input_filepaths : typing.List[pathlib.Path] = [], open_balance : float = 0.0, csv_format : str = "") -> bool :
        new_account = Account(output_filepath.stem, open_balance, transactions_from_csvs(input_filepaths, csv_format))

        with open(output_filepath, 'x') as _ :
            pass

        json_write(output_filepath, new_account)
        self.base_account_lookup[output_filepath.stem] = make_managed_account(new_account)

LedgerEntryType = typing.Tuple[LedgerTransaction, LedgerTransaction]

class Ledger :

    def __init__(self) :
        self.ledger_entries : typing.List[LedgerEntryType] = []
        self.transaction_lookup : typing.Set[int] = set()

    def add_to_ledger(self, from_account_name : str, from_transaction : Transaction, to_account_name : str, to_transaction : Transaction) :
        debug_assert(from_account_name != to_account_name, "Transaction to same account forbidden!")
        debug_assert(from_transaction.delta == -to_transaction.delta, "Transaction is not balanced credit and debit!")

        #insert from - to
        new_ledger_entry : LedgerEntryType = (LedgerTransaction(from_account_name, from_transaction), LedgerTransaction(to_account_name, to_transaction))
        self.ledger_entries.add(new_ledger_entry)
        self.transaction_lookup.add(from_transaction.ID)
        self.transaction_lookup.add(to_transaction.ID)

    def transaction_accounted(self, transaction_ID : int) -> bool :
        return (transaction_ID in self.transaction_lookup)

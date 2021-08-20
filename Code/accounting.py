import pathlib
import typing

from accounting_objects import Transaction, LedgerTransaction, Account, AccountDataTable
from json_file import json_read, json_write
from debug import debug_assert, debug_message

data_path = pathlib.Path("Data")
if not data_path.exists() :
    data_path.mkdir()
ledger_data_path = data_path.joinpath("SomeLedger")
if not ledger_data_path.exists() :
    ledger_data_path.mkdir()
transaction_base_data_path = ledger_data_path.joinpath("BaseAccounts")
transaction_derived_data_path = ledger_data_path.joinpath("DerivedAccounts")
if not transaction_derived_data_path.exists() :
    transaction_derived_data_path.mkdir()

class Ledger :

    EntryType = typing.Tuple[LedgerTransaction, LedgerTransaction]

    def __init__(self) :
        self.ledger_entries : typing.List[Ledger.EntryType] = []
        self.transaction_lookup : typing.Set[int] = set()

    def add_to_ledger(self, from_account_name : str, from_transaction : Transaction, to_account_name : str, to_transaction : Transaction) :
        debug_assert(from_account_name != to_account_name, "Transaction to same account forbidden!")
        debug_assert(from_transaction.delta == -to_transaction.delta, "Transaction is not balanced credit and debit!")

        #insert from - to
        new_ledger_entry : Ledger.EntryType = (LedgerTransaction(from_account_name, from_transaction), LedgerTransaction(to_account_name, to_transaction))
        self.ledger_entries.add(new_ledger_entry)
        self.transaction_lookup.add(from_transaction.ID)
        self.transaction_lookup.add(to_transaction.ID)

    def transaction_accounted(self, transaction_ID : int) -> bool :
        return (transaction_ID in self.transaction_lookup)


class AccountManager :

    class AccountDataAndTable :

        def __init__(self, account_data : Account) :
            self.account_data = account_data
            self.account_table_data = AccountDataTable(account_data)


    def __init__(self) :
        loaded_base_accounts = AccountManager.__load_accounts(transaction_base_data_path)

        self.base_account_lookup : typing.Mapping[str, AccountManager.AccountDataAndTable] = {}
        for account in loaded_base_accounts :
            self.base_account_lookup[account.name] = AccountManager.AccountDataAndTable(account)

        loaded_derived_accounts = AccountManager.__load_accounts(transaction_derived_data_path)

        self.derived_account_lookup : typing.Mapping[str, AccountManager.AccountDataAndTable] = {}
        for account in loaded_derived_accounts :
            self.derived_account_lookup[account.name] = AccountManager.AccountDataAndTable(account)

    @staticmethod
    def __load_accounts(account_directory : pathlib.Path) -> typing.List[Account] :

        account_list = []
        for account_folder_entry in account_directory.iterdir() :
            if account_folder_entry.is_file() :
                account_list.append(json_read(account_folder_entry))
            elif account_folder_entry.is_dir() :
                account_list.extend(AccountManager.__load_accounts(account_folder_entry))
            else :
                debug_message(f"Could not handle directory entry named \"{account_folder_entry}\"")

        return account_list

    def __get_account_data_pair(self, account_name : str) -> AccountDataAndTable :
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
            return data_set.account_data
        return None

    def get_account_table(self, account_name : str) -> AccountDataTable :
        data_set = self.__get_account_data_pair(account_name)
        if data_set is not None :
            return data_set.account_table_data
        return None

    def create_derived_account(self, account_name : str, transactions : typing.List[Transaction] = [], in_directory = transaction_derived_data_path) -> bool :
        new_account = Account(account_name, transactions=transactions)
        new_account.update_hash()
        write_file_path : pathlib.Path = in_directory.joinpath(account_name + ".json")

        if not in_directory.exists() :
            in_directory.mkdir(parents=True)

        with open(write_file_path, 'x') as output_file :
            pass

        json_write(write_file_path, new_account)
        self.derived_account_lookup[account_name] = AccountManager.AccountDataAndTable(new_account)


#json_register_writeable(Ledger)
#json_register_readable(Ledger)
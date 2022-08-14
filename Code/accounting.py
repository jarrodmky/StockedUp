import pathlib
import typing
from pandas import DataFrame, Series

from accounting_objects import Transaction, Account, LedgerEntry, UniqueHashCollector
from json_file import json_read, json_write, json_register_readable
from debug import debug_assert, debug_message
from csv_importing import read_transactions_from_csvs
from utf8_file import utf8_file

class AccountMapping :

    class Matching :

        def __init__(self) :
            pass

        @staticmethod
        def decode(reader) :
            new_matching = AccountMapping.Matching()
            new_matching.account_name = reader["account name"]
            new_matching.strings = reader["strings"]
            return new_matching

    def __init__(self) :
        pass

    @staticmethod
    def decode(reader) :
        new_account_mapping = AccountMapping()
        new_account_mapping.name = reader["name"]
        new_account_mapping.matchings = reader["matchings"]
        return new_account_mapping

json_register_readable(AccountMapping)
json_register_readable(AccountMapping.Matching)


AccountList = typing.List[Account]

# raw account, display table, is derived
ManagedAccountData = typing.Tuple[Account, DataFrame, bool]

def make_managed_account(account_data : Account, is_derived : bool) -> ManagedAccountData :
    return (account_data, account_data.make_account_data_table(), is_derived)

class AccountManager :

    def __init__(self, ledger_path : pathlib.Path) :

        self.hash_register = UniqueHashCollector()

        self.base_account_data_path = ledger_path.joinpath("BaseAccounts")
        if not self.base_account_data_path.exists() :
            self.base_account_data_path.mkdir()

        self.derived_account_data_path = ledger_path.joinpath("DerivedAccounts")
        if not self.derived_account_data_path.exists() :
            self.derived_account_data_path.mkdir()

        self.account_lookup : typing.Dict[str, ManagedAccountData] = {}
        for account in AccountManager.__load_accounts_from_directory(self.base_account_data_path) :
            self.account_lookup[account.name] = make_managed_account(account, False)

        for account in AccountManager.__load_accounts_from_directory(self.derived_account_data_path) :
            self.account_lookup[account.name] = make_managed_account(account, True)

    def __get_account_data_pair(self, account_name : str) -> ManagedAccountData :
        assert self.account_is_created(account_name)
        return self.account_lookup[account_name]

    def get_account_names(self) -> typing.List[str] :
        return sorted(list(self.account_lookup.keys()))

    def account_is_created(self, account_name : str) -> bool :
        return account_name in self.account_lookup

    def get_account_data(self, account_name : str) -> Account :
        return self.__get_account_data_pair(account_name)[0]

    def get_account_table(self, account_name : str) -> DataFrame :
        return self.__get_account_data_pair(account_name)[1]

    def get_account_is_derived(self, account_name : str) -> bool :
        return self.__get_account_data_pair(account_name)[2]

    def __create_Account_file(self, file_path : pathlib.Path, account : Account, is_derived : bool) -> None :
        with open(file_path, 'x') as _ :
            pass

        json_write(file_path, account)
        self.account_lookup[account.name] = make_managed_account(account, is_derived)


    def create_account_from_transactions(self, account_name : str, transactions : typing.List[Transaction] = [], open_balance : float = 0.0) -> None :
        assert(not self.account_is_created(account_name))
        account_file_path = self.derived_account_data_path.joinpath(account_name + ".json")
        new_account = Account(account_name, open_balance, transactions)
        new_account.update_hash(self.hash_register)
        self.__create_Account_file(account_file_path, new_account, True)

    def create_account_from_csvs(self, account_name : str, input_filepaths : typing.List[pathlib.Path] = [], open_balance : float = 0.0) -> None :
        assert(not self.account_is_created(account_name))
        account_file_path = self.base_account_data_path.joinpath(account_name + ".json")
        new_account = Account(account_name, open_balance, read_transactions_from_csvs(input_filepaths))
        new_account.update_hash(self.hash_register)
        self.__create_Account_file(account_file_path, new_account, False)

    def delete_account(self, account_name : str) -> None :
        assert(self.account_is_created(account_name))

        file_deleted = False

        base_account_file_path = self.base_account_data_path.joinpath(account_name + ".json")
        if base_account_file_path.exists() :
            base_account_file_path.unlink()
            file_deleted = True

        derived_account_file_path = self.derived_account_data_path.joinpath(account_name + ".json")
        if derived_account_file_path.exists() :
            debug_assert(not file_deleted)
            derived_account_file_path.unlink()
            file_deleted = True
            
        debug_assert(file_deleted)

        del self.account_lookup[account_name]



    @staticmethod
    def __load_accounts_from_directory(account_directory : pathlib.Path) -> AccountList :
        account_list = []
        for account_folder_entry in account_directory.iterdir() :
            if account_folder_entry.is_file() :
                account_list.append(json_read(account_folder_entry))
            elif account_folder_entry.is_dir() :
                account_list.extend(AccountManager.__load_accounts_from_directory(account_folder_entry))
            else :
                debug_message(f"Could not handle directory entry named \"{account_folder_entry}\"")
        return account_list

TransactionPair = typing.Tuple[str, Transaction, str, Transaction] #from-to pair

class Ledger(AccountManager) :

    def __init__(self, ledger_path : pathlib.Path) :
        AccountManager.__init__(self, ledger_path)

        self.ledger_entries : typing.List[LedgerEntry] = []
        self.transaction_lookup : typing.Set[int] = set()

        self.account_mapping_file_path = ledger_path.parent.joinpath("Accounting.json")
        if not self.account_mapping_file_path.exists() :
            with utf8_file(self.account_mapping_file_path, 'x') as new_mapping_file :
                new_mapping_file.write("{\n")
                new_mapping_file.write("\t\"mappings\": []\n")
                new_mapping_file.write("}")

        
        self.ledger_entries_file_path = ledger_path.joinpath("LedgerEntries.json")
        if not self.ledger_entries_file_path.exists() :
            with utf8_file(self.ledger_entries_file_path, 'x') as new_entry_file :
                new_entry_file.write("{\n")
                new_entry_file.write("\t\"entries\": []\n")
                new_entry_file.write("}")
        else :
            self.ledger_entries = json_read(self.ledger_entries_file_path)["entries"]
            for entry in self.ledger_entries :
                self.transaction_lookup.add(entry.from_transaction.ID)
                self.transaction_lookup.add(entry.to_transaction.ID)

    def __transaction_accounted(self, transaction_ID : int) -> bool :
        debug_assert(len(self.transaction_lookup) > 0)
        return (transaction_ID in self.transaction_lookup)

    def map_spending_accounts(self) :
        account_mapping_list : typing.List[AccountMapping] = json_read(self.account_mapping_file_path)["mappings"]
        transaction_pairings : typing.List[TransactionPair] = []

        for account_mapping in account_mapping_list :
            debug_message(f"Mapping spending account \"{account_mapping.name}\"")
            debug_assert(not self.account_is_created(account_mapping.name), "Account already created! Can only map to new account")

            matching_transactions = []

            if len(account_mapping.matchings) > 0 and account_mapping.matchings[0] is not None and account_mapping.matchings[0].account_name == "" :
                universal_matching_strings = account_mapping.matchings[0].strings
                for account_name in self.get_account_names() :
                    if not self.get_account_is_derived(account_name) :
                        match_account = self.get_account_data(account_name)
                        debug_assert(match_account is not None, "Account not found! Expected account \"" + match_account.name + "\"")
                        
                        for transaction in match_account.transactions :
                            for matching_string in universal_matching_strings :
                                if matching_string in transaction.description :
                                    derived_transaction = Transaction(transaction.date, transaction.timestamp, -transaction.delta, transaction.description)
                                    matching_transactions.append(derived_transaction)
                                    transaction_pairings.append((account_name, transaction, account_mapping.name, derived_transaction))
                                    break

            else :
                for matching in account_mapping.matchings :
                    match_account = self.get_account_data(matching.account_name)
                    debug_assert(match_account is not None, "Account not found! Expected account \"" + match_account.name + "\"")

                    for transaction in match_account.transactions :
                        for matching_string in matching.strings :
                            if matching_string in transaction.description :
                                derived_transaction = Transaction(transaction.date, transaction.timestamp, -transaction.delta, transaction.description)
                                matching_transactions.append(derived_transaction)
                                transaction_pairings.append((matching.account_name, transaction, account_mapping.name, derived_transaction))
                                break

            self.create_account_from_transactions(account_mapping.name, matching_transactions)

        for (from_accnt, from_trnsctn, to_accnt, to_trnsctn) in transaction_pairings :
            self.transaction_lookup.add(from_trnsctn.ID)
            self.transaction_lookup.add(to_trnsctn.ID)
            new_ledger_entry : LedgerEntry = LedgerEntry.create(from_accnt, from_trnsctn, to_accnt, to_trnsctn)
            self.ledger_entries.append(new_ledger_entry)

        json_write(self.ledger_entries_file_path, {"entries" : self.ledger_entries})

    def get_unaccounted_transaction_table(self) -> DataFrame :
        unaccounted_transaction_list = []
        corresponding_account_list = []
        for account_name in self.get_account_names() :
            if not self.get_account_is_derived(account_name) :
                account_data = self.get_account_data(account_name)
                for transaction in account_data.transactions :
                    if not self.__transaction_accounted(transaction.ID) :
                        unaccounted_transaction_list.append(transaction)
                        corresponding_account_list.append(account_name)
        unaccouted_table = DataFrame([{ "ID" : t.ID, "Date" : t.date, "Delta" : t.delta, "Description" : t.description } for t in unaccounted_transaction_list])
        return unaccouted_table.join(Series(corresponding_account_list, name="Account"))

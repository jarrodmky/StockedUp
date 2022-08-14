import pathlib
from tkinter.messagebox import NO
import typing
from pandas import DataFrame, Series

from accounting_objects import Transaction, Account, LedgerEntry, UniqueHashCollector
from json_file import json_read, json_write, json_register_readable
from debug import debug_assert, debug_message
from csv_importing import read_transactions_from_csvs
from utf8_file import utf8_file

class DerivedAccount :

    class Matching :

        def __init__(self) :
            pass

        @staticmethod
        def decode(reader) :
            new_matching = DerivedAccount.Matching()
            new_matching.account_name = reader["account name"]
            new_matching.strings = reader["strings"]
            return new_matching

    def __init__(self) :
        pass

    @staticmethod
    def decode(reader) :
        new_derived_account = DerivedAccount()
        new_derived_account.name = reader["name"]
        new_derived_account.matchings = reader["matchings"]
        return new_derived_account

json_register_readable(DerivedAccount)
json_register_readable(DerivedAccount.Matching)

class InternalTransactionMapping :

    def __init__(self) :
        pass

    @staticmethod
    def decode(reader) :
        new_transaction_mapping = InternalTransactionMapping()
        new_transaction_mapping.from_account = reader["from account"]
        new_transaction_mapping.from_match_strings = reader["from matchings"]
        new_transaction_mapping.to_account = reader["to account"]
        new_transaction_mapping.to_match_strings = reader["to matchings"]
        return new_transaction_mapping

json_register_readable(InternalTransactionMapping)


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
        assert not self.account_is_created(account_name), f"Account {account_name} already exists!"
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


class TransactionManager :

    def __init__(self) :
        self.transaction_lookup : typing.Set[int] = set()

    def transaction_accounted(self, transaction_ID : int) -> bool :
        return (transaction_ID in self.transaction_lookup)

    def account_transaction(self, transaction_ID : int) -> None :
        assert transaction_ID not in self.transaction_lookup, f"Duplicate unique hash already existing in ledger {transaction_ID}! Double matched somehow?"
        self.transaction_lookup.add(transaction_ID)

class AccountSearcher :

    TransactionMatch = typing.Tuple[str, int] #(account, index)
    TransactionPair = typing.Tuple[str, Transaction, str, Transaction] #from-to (account, transaction) pair
    
    def __init__(self) :
        self.matching_transactions : typing.List[AccountSearcher.TransactionMatch] = []

    def check_account(self, account_manager : AccountManager, transaction_manager : TransactionManager, account_name : str, string_matches : typing.List[str]) -> None :
        match_account = account_manager.get_account_data(account_name)
        debug_assert(match_account is not None, "Account not found! Expected account \"" + match_account.name + "\" to exist!")
        debug_message(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
        for index, transaction in enumerate(match_account.transactions) :
            for matching_string in string_matches :
                if matching_string in transaction.description and not transaction_manager.transaction_accounted(transaction.ID) :
                    self.matching_transactions.append((account_name, index))
                    break

    def derive_account_from_findings(self, account_manager : AccountManager, create_account_name : str) -> typing.List[LedgerEntry] :
        mirror_transactions = []
        transaction_pairings : typing.List[AccountSearcher.TransactionPair] = []
        for account_name, index in self.matching_transactions :
            transaction = account_manager.get_account_data(account_name).transactions[index]
            mirror_transaction = Transaction(transaction.date, transaction.timestamp, -transaction.delta, transaction.description)
            mirror_transactions.append(mirror_transaction)
            transaction_pairings.append((account_name, transaction, create_account_name, mirror_transaction))

        if len(mirror_transactions) > 0 :
            account_manager.create_account_from_transactions(create_account_name, mirror_transactions)

            ledger_entries = []
            for (from_accnt, from_trnsctn, to_accnt, to_trnsctn) in transaction_pairings :
                ledger_entries.append(LedgerEntry.create(from_accnt, from_trnsctn, to_accnt, to_trnsctn))
            return ledger_entries
        else :
            return []

class Ledger(AccountManager, TransactionManager) :

    def __init__(self, ledger_path : pathlib.Path) :
        AccountManager.__init__(self, ledger_path)
        TransactionManager.__init__(self)

        self.ledger_entries : typing.List[LedgerEntry] = []

        self.account_mapping_file_path = ledger_path.parent.joinpath("Accounting.json")
        if not self.account_mapping_file_path.exists() :
            with utf8_file(self.account_mapping_file_path, 'x') as new_mapping_file :
                new_mapping_file.write("{\n")
                new_mapping_file.write("\t\"derived accounts\": [],\n")
                new_mapping_file.write("\t\"internal transactions\": []\n")
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
                self.account_transaction(entry.from_transaction.ID)
                self.account_transaction(entry.to_transaction.ID)

    def create_derived_accounts(self) :
        account_mapping_list : typing.List[DerivedAccount] = json_read(self.account_mapping_file_path)["derived accounts"]

        for account_mapping in account_mapping_list :
            debug_message(f"Mapping spending account \"{account_mapping.name}\"")
            if self.account_is_created(account_mapping.name) :
                self.delete_account(account_mapping.name)

            deriver = AccountSearcher()

            if len(account_mapping.matchings) == 1 and account_mapping.matchings[0] is not None and account_mapping.matchings[0].account_name == "" :
                universal_match_strings = account_mapping.matchings[0].strings
                debug_message(f"Checking all base accounts for {universal_match_strings}")
                for account_name in self.get_account_names() :
                    if not self.get_account_is_derived(account_name) :
                        deriver.check_account(self, self, account_name, universal_match_strings)
            else :
                for matching in account_mapping.matchings :
                    if matching.account_name == "" :
                        raise RuntimeError(f"Nonspecific match strings detected for account {account_mapping.name}! Not compatible with specified accounts!")
                    debug_message(f"Checking {matching.account_name} accounts for {matching.strings}")
                    deriver.check_account(self, self, matching.account_name, matching.strings)

            generated_ledger_entries = deriver.derive_account_from_findings(self, account_mapping.name)

            for ledger_entry in generated_ledger_entries :
                self.account_transaction(ledger_entry.from_transaction.ID)
                self.account_transaction(ledger_entry.to_transaction.ID)

            self.ledger_entries.extend(generated_ledger_entries)

            if(len(generated_ledger_entries) > 0) :
                debug_message("... account mapped!")
            else :
                debug_message("... nothing to map!")
    
    def account_internal_transactions(self) :
        transaction_mapping_list : typing.List[InternalTransactionMapping] = json_read(self.account_mapping_file_path)["internal transactions"]
        for mapping in transaction_mapping_list :
            debug_message(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
            from_finder = AccountSearcher()
            from_finder.check_account(self, self, mapping.from_account, mapping.from_match_strings)
            to_finder = AccountSearcher()
            to_finder.check_account(self, self, mapping.to_account, mapping.to_match_strings)
            
            #assumes in order on both accounts
            for (from_account, from_index), (to_account, to_index) in zip(from_finder.matching_transactions, to_finder.matching_transactions) :
                from_transaction = self.get_account_data(from_account).transactions[from_index]
                to_transaction = self.get_account_data(to_account).transactions[to_index]
                if from_transaction.delta != -to_transaction.delta :
                    debug_message(f"Not in sync! Tried:\n\t{from_transaction.encode()}\nTo:\n\t{to_transaction.encode()}")
                else :
                    self.ledger_entries.append(LedgerEntry.create(from_account, from_transaction, to_account, to_transaction))
                    self.account_transaction(from_transaction.ID)
                    self.account_transaction(to_transaction.ID)

    def clear(self) :
        self.ledger_entries = []
        self.transaction_lookup = set()

    def save(self) :
        json_write(self.ledger_entries_file_path, {"entries" : self.ledger_entries})

    def get_unaccounted_transaction_table(self) -> DataFrame :
        unaccounted_transaction_list = []
        corresponding_account_list = []
        for account_name in self.get_account_names() :
            if not self.get_account_is_derived(account_name) :
                account_data = self.get_account_data(account_name)
                for transaction in account_data.transactions :
                    if not self.transaction_accounted(transaction.ID) :
                        unaccounted_transaction_list.append(transaction)
                        corresponding_account_list.append(account_name)
        unaccouted_table = DataFrame([{ "ID" : t.ID, "Date" : t.date, "Delta" : t.delta, "Description" : t.description } for t in unaccounted_transaction_list])
        return unaccouted_table.join(Series(corresponding_account_list, name="Account"))

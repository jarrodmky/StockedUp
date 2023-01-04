import pathlib
import typing
from graphlib import TopologicalSorter, CycleError

from pandas import DataFrame, Series

from PyJMy.json_file import json_read, json_write, json_register_readable
from PyJMy.debug import debug_assert, debug_message
from PyJMy.utf8_file import utf8_file

from accounting_objects import Transaction, Account, LedgerEntry, UniqueHashCollector
from csv_importing import read_transactions_from_csvs

class DerivedAccount :

    class Matching :

        def __init__(self) :
            self.account_name : str = "<INVALID ACCOUNT>"
            self.strings : typing.List[str] = []

        @staticmethod
        def decode(reader) :
            new_matching = DerivedAccount.Matching()
            new_matching.account_name = reader["account name"]
            new_matching.strings = reader["strings"]
            return new_matching

    def __init__(self) :
        self.name = "<INVALID ACCOUNT>"
        self.matchings : typing.List[DerivedAccount.Matching] = []
        self.start_value = 0.0

    @staticmethod
    def decode(reader) :
        new_derived_account = DerivedAccount()
        new_derived_account.name = reader["name"]
        new_derived_account.matchings = reader["matchings"]
        new_derived_account.start_value = reader.read_optional("starting value", 0.0)
        return new_derived_account

json_register_readable(DerivedAccount)
json_register_readable(DerivedAccount.Matching)

class InternalTransactionMapping :

    def __init__(self) :
        self.from_account = "<INVALID ACCOUNT>"
        self.from_match_strings : typing.List[str] = []
        self.to_account = "<INVALID ACCOUNT>"
        self.to_match_strings : typing.List[str] = []

    @staticmethod
    def decode(reader) :
        new_transaction_mapping = InternalTransactionMapping()
        new_transaction_mapping.from_account = reader["from account"]
        new_transaction_mapping.from_match_strings = reader["from matchings"]
        new_transaction_mapping.to_account = reader["to account"]
        new_transaction_mapping.to_match_strings = reader["to matchings"]
        return new_transaction_mapping

json_register_readable(InternalTransactionMapping)

class NameTreeNode :

    def __init__(self) :
        self.node_name = "INVALID NODE"
        self.children_names = []

    @staticmethod
    def decode(reader) :
        new_node = NameTreeNode()
        new_node.node_name = reader["name"]
        new_node.children_names = reader["children"]
        return new_node

json_register_readable(NameTreeNode)


AccountList = typing.List[Account]

# raw account, display table, is derived
ManagedAccountData = typing.Tuple[Account, DataFrame, bool]

AccountIndex = typing.Tuple[str, int]
AccountIndexList = typing.List[AccountIndex]

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

    def get_base_account_names(self) -> typing.List[str] :
        return [name for name in self.get_account_names() if not self.get_account_is_derived(name)]

    def get_derived_account_names(self) -> typing.List[str] :
        return [name for name in self.get_account_names() if self.get_account_is_derived(name)]

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


class TransactionAccounter :

    def __init__(self) :
        self.transaction_lookup : typing.Set[int] = set()

    def transaction_accounted(self, transaction_ID : int) -> bool :
        return (transaction_ID in self.transaction_lookup)

    def account_transaction(self, transaction_ID : int) -> None :
        assert transaction_ID not in self.transaction_lookup, f"Duplicate unique hash already existing in ledger {transaction_ID}! Double matched somehow?"
        self.transaction_lookup.add(transaction_ID)

class AccountSearcher :

    TransactionPair = typing.Tuple[str, Transaction, str, Transaction] #from-to (account, transaction) pair
    
    def __init__(self) :
        self.matching_transactions : AccountIndexList = []

    def check_account(self, account_manager : AccountManager, transaction_accounter : TransactionAccounter, account_name : str, string_matches : typing.List[str]) -> None :
        match_account = account_manager.get_account_data(account_name)
        debug_assert(match_account is not None, "Account not found! Expected account \"" + match_account.name + "\" to exist!")
        debug_message(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
        prior_count = len(self.matching_transactions)
        for index, transaction in enumerate(match_account.transactions) :
            for matching_string in string_matches :
                if matching_string in transaction.description :
                    debug_assert(not transaction_accounter.transaction_accounted(transaction.ID), f"Transaction already accounted! : {transaction.encode()}")
                    self.matching_transactions.append((account_name, index))
                    break
        debug_message(f"Found {len(self.matching_transactions) - prior_count} transactions in {account_name}")

class NameTree :

    def __init__(self, node_data : typing.List[NameTreeNode]) :
        assert len(node_data) > 0, "No nodes, did you mean to pass some data?"

        self.node_dictionary : typing.Dict[str, typing.List[str]] = {}
        self.root_node = node_data[0].node_name

        #build node dictionary and check for duplicates
        for node in node_data :
            assert node.node_name not in self.node_dictionary
            for child_name in node.children_names :
                assert child_name not in self.node_dictionary, f"Duplicate interior node found or node specification is out of order at \"{child_name}\""
            self.node_dictionary[node.node_name] = node.children_names

        #check for disconnected nodes
        child_node_set = set()
        for node in node_data :
            for child_name in node.children_names :
                assert child_name not in child_node_set, f"Found duplicate child node \"{child_name}\""
                child_node_set.add(child_name)
        
        for node in node_data[1:] :
            assert node.node_name in child_node_set, f"Found isolated node \"{node.node_name}\""

        #build tree
        topological_sorter = TopologicalSorter(self.node_dictionary)

        #check no cycles
        try :
            topological_sorter.prepare()
        except CycleError :
            assert False, "Cycle detected!"

    def get_root(self) :
        return self.root_node

    def get_children(self, node_name) :
        assert node_name in self.node_dictionary, f"Could not find node '{node_name}', maybe it has no transactions?"
        return self.node_dictionary[node_name]

class Ledger(AccountManager, TransactionAccounter) :

    def __init__(self, ledger_path : pathlib.Path) :
        AccountManager.__init__(self, ledger_path)
        TransactionAccounter.__init__(self)
        assert ledger_path.exists() or not ledger_path.is_dir(), "Expected ledger path not found!"

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

        self.initialize_category_tree()

    def clear(self) :
        self.ledger_entries = []
        self.transaction_lookup = set()

    def save(self) :
        json_write(self.ledger_entries_file_path, {"entries" : self.ledger_entries})

    def initialize_category_tree(self) :
        tree_nodes : typing.List[NameTreeNode] = json_read(self.account_mapping_file_path)["derived account category tree"]
        self.category_tree = NameTree(tree_nodes)

        base_account_set = set(self.get_base_account_names())
        derived_account_set = set(self.get_derived_account_names())

        topological_sorter = TopologicalSorter(self.category_tree.node_dictionary)
        for node in topological_sorter.static_order() :
            assert node not in base_account_set, "Base accounts cannot be in the category tree!"
            derived_account_set.discard(node)

        assert len(derived_account_set) == 0, f"Not all derived accounts in tree! Missing ({derived_account_set})"

    def __derive_account(self, account_name : str, base_transaction_indices : AccountIndexList, open_balance : float) -> None :
        derived_transactions = []
        transaction_pairings : typing.List[AccountSearcher.TransactionPair] = []
        for base_account_name, index in base_transaction_indices :
            transaction = self.get_account_data(base_account_name).transactions[index]
            derived_transaction = Transaction(transaction.date, transaction.timestamp, -transaction.delta, transaction.description)
            derived_transactions.append(derived_transaction)
            transaction_pairings.append((base_account_name, transaction, account_name, derived_transaction))

        if len(derived_transactions) > 0 :
            self.create_account_from_transactions(account_name, derived_transactions, open_balance)

            for (from_accnt, from_trnsctn, to_accnt, to_trnsctn) in transaction_pairings :
                self.account_transaction(from_trnsctn.ID)
                self.account_transaction(to_trnsctn.ID)
                self.ledger_entries.append(LedgerEntry.create(from_accnt, from_trnsctn, to_accnt, to_trnsctn))
            
            debug_message(f"... account {account_name} derived!")
        else :
            debug_message(f"... nothing to map for {account_name}!")

    def __validate_internal_account_mapping(self, from_account_name : str, from_transaction_indices : AccountIndexList, to_account_name : str, to_transaction_indices : AccountIndexList) -> None :
            
        #assumes in order on both accounts
        for (from_account, from_index), (to_account, to_index) in zip(from_transaction_indices, to_transaction_indices) :
            from_transaction = self.get_account_data(from_account).transactions[from_index]
            to_transaction = self.get_account_data(to_account).transactions[to_index]
            if from_transaction.delta != -to_transaction.delta :
                debug_message(f"Not in sync! Tried:\n\t{from_transaction.encode()}\nTo:\n\t{to_transaction.encode()}")
            else :
                self.ledger_entries.append(LedgerEntry.create(from_account, from_transaction, to_account, to_transaction))
                self.account_transaction(from_transaction.ID)
                self.account_transaction(to_transaction.ID)
              
        #print missing transactions
        from_transaction_count = len(from_transaction_indices)
        to_transaction_count = len(to_transaction_indices)  
        if from_transaction_count > to_transaction_count :
            difference = from_transaction_count - to_transaction_count
            missing_transactions = [self.get_account_data(acct).transactions[idx] for (acct, idx) in from_transaction_indices[-difference:]]
            debug_message(f"\"{to_account_name}\" missing {difference} transactions:\n{[t.encode() for t in missing_transactions]}")
        elif from_transaction_count < to_transaction_count :
            difference = to_transaction_count - from_transaction_count
            missing_transactions = [self.get_account_data(acct).transactions[idx] for (acct, idx) in to_transaction_indices[-difference:]]
            debug_message(f"\"{from_account_name}\" missing {difference} transactions:\n{[t.encode() for t in missing_transactions]}")

        if from_transaction_count == 0 or to_transaction_count == 0 :
            debug_message("... nothing to map!")
        else :
            debug_message("... account mapped!")

    def derive_and_balance_accounts(self) :

        #derived account matchings
        account_mapping_list : typing.List[DerivedAccount] = json_read(self.account_mapping_file_path)["derived accounts"]
        for account_mapping in account_mapping_list :
            debug_message(f"Mapping spending account \"{account_mapping.name}\"")
            if self.account_is_created(account_mapping.name) :
                self.delete_account(account_mapping.name)

            deriver = AccountSearcher()

            if len(account_mapping.matchings) == 1 and account_mapping.matchings[0] is not None and account_mapping.matchings[0].account_name == "" :
                universal_match_strings = account_mapping.matchings[0].strings
                debug_message(f"Checking all base accounts for {universal_match_strings}")
                for account_name in self.get_base_account_names() :
                    deriver.check_account(self, self, account_name, universal_match_strings)
            else :
                for matching in account_mapping.matchings :
                    if matching.account_name == "" :
                        raise RuntimeError(f"Nonspecific match strings detected for account {account_mapping.name}! Not compatible with specified accounts!")
                    debug_message(f"Checking {matching.account_name} accounts for {matching.strings}")
                    deriver.check_account(self, self, matching.account_name, matching.strings)
            
            self.__derive_account(account_mapping.name, deriver.matching_transactions, account_mapping.start_value)

        #internal transaction mappings
        transaction_mapping_list : typing.List[InternalTransactionMapping] = json_read(self.account_mapping_file_path)["internal transactions"]
        for mapping in transaction_mapping_list :
            debug_message(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
            from_finder = AccountSearcher()
            from_finder.check_account(self, self, mapping.from_account, mapping.from_match_strings)
            to_finder = AccountSearcher()
            to_finder.check_account(self, self, mapping.to_account, mapping.to_match_strings)

            self.__validate_internal_account_mapping(mapping.from_account, from_finder.matching_transactions, mapping.to_account, to_finder.matching_transactions)

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
        unaccouted_table = DataFrame([{ "Index" : idx, "Date" : t.date, "Description" : t.description, "Delta" : t.delta } for idx, t in enumerate(unaccounted_transaction_list)])
        return unaccouted_table.join(Series(corresponding_account_list, name="Account"))

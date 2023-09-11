import pathlib
import typing
from graphlib import TopologicalSorter, CycleError
from numpy import repeat

from pandas import DataFrame, Series, Index, concat

from PyJMy.json_file import json_read, json_write
from PyJMy.debug import debug_assert, debug_message
from PyJMy.utf8_file import utf8_file

from csv_importing import read_transactions_from_csv_in_path
from xls_importing import read_transactions_from_xls

from accounting_objects import Account, LedgerEntry, UniqueHashCollector, LedgerTransaction, make_hasher
from accounting_objects import NameTreeNode, LedgerImport, AccountImport, DerivedAccount, InternalTransactionMapping

AccountList = typing.List[Account]

# raw account, display table, is derived
ManagedAccountData = typing.Tuple[Account, DataFrame, bool]

AccountIndex = typing.Tuple[str, int]

def transaction_hash(index : int, date : str, timestamp : float, delta : float, description : str) -> int :
    hasher = make_hasher()
    
    hasher.update(date.encode())
    tsNum, tsDen = timestamp.as_integer_ratio()
    hasher.update(tsNum.to_bytes(8, 'big', signed=True))
    hasher.update(tsDen.to_bytes(8, 'big'))
    dtNum, dtDen = delta.as_integer_ratio()
    hasher.update(dtNum.to_bytes(8, 'big', signed=True))
    hasher.update(dtDen.to_bytes(8, 'big'))
    hasher.update(description.encode())
    new_id = int.from_bytes(hasher.digest(12), 'big')
    new_id <<= 32 #(4*8) pad 4 bytes
    new_id += index
    return new_id

def make_managed_account(account_data : Account, is_derived : bool) -> ManagedAccountData :
    return (account_data, account_data.make_account_data_table(), is_derived)

derived_transaction_columns = ["date", "delta", "description", "timestamp", "source_ID", "source_account"]
unidentified_transaction_columns = ["date", "delta", "description", "timestamp"]
transaction_columns = ["ID", "date", "delta", "description", "timestamp"]

def make_identified_transaction_dataframe(transactions : DataFrame) -> DataFrame :
    make_id = lambda t : transaction_hash(int(t.name), t.date, t.timestamp, t.delta, t.description)
    transactions.insert(0, "ID", transactions.apply(make_id, axis=1, result_type="reduce"))
    return transactions

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

    def create_account_from_transactions(self, root_path : pathlib.Path, is_derived : bool, account_name : str, transactions : DataFrame, open_balance : float) -> None :
        assert transactions.columns.equals(Index(transaction_columns))
        assert not self.account_is_created(account_name), f"Account {account_name} already exists!"
        account_file_path = root_path.joinpath(account_name + ".json")
        account = Account(self.hash_register, account_name, open_balance, transactions)

        #write to file
        with open(account_file_path, 'x') as _ :
            pass
        json_write(account_file_path, account)
        self.account_lookup[account.name] = make_managed_account(account, is_derived)

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
    
    def __init__(self) :
        self.__matching_transactions : DataFrame = None

    def _get_match_tuples(self, account_transactions: DataFrame, _ : str, string_matches: typing.List[str]) -> DataFrame :

        escape_string = lambda s : s.replace("*", "\*").replace("+", "\+").replace("(", "\(").replace(")", "\)")
        regex_pattern = "|".join([escape_string(s) for s in string_matches])
        found_transactions = account_transactions[account_transactions["description"].str.contains(regex_pattern)]

        return found_transactions if not found_transactions.empty else DataFrame(columns=transaction_columns)

    def check_account(self, account_manager : AccountManager, account_name : str, string_matches : typing.List[str]) -> None :
        match_account = account_manager.get_account_data(account_name)
        debug_assert(match_account is not None, "Account not found! Expected account \"" + match_account.name + "\" to exist!")
        debug_message(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
        prior_count = 0 if self.__matching_transactions is None else len(self.__matching_transactions.index)
        match_tuples = self._get_match_tuples(match_account.transactions, match_account.name, string_matches)
        self.__matching_transactions = concat([self.__matching_transactions, match_tuples], ignore_index=True)
        debug_message(f"Found {len(self.__matching_transactions) - prior_count} transactions in {account_name}")

    def get_matching_transactions(self) -> DataFrame :
        return self.__matching_transactions
    
class AccountDeriver(AccountSearcher) :

    def _get_match_tuples(self, account_transactions: DataFrame, source_account_name : str, string_matches: typing.List[str]) -> DataFrame:
        found_transactions = super()._get_match_tuples(account_transactions, source_account_name, string_matches)
        return DataFrame({
                "date" : found_transactions["date"],
                "delta" : -found_transactions["delta"],
                "description" : found_transactions["description"],
                "timestamp" : found_transactions["timestamp"],
                "source_ID" : found_transactions["ID"],
                "source_account" : repeat(source_account_name, len(found_transactions))
            })

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

    def __init__(self, ledger_data_path : pathlib.Path, ledger_import : LedgerImport) :
        assert ledger_data_path.exists() or not ledger_data_path.is_dir(), "Expected ledger path not found!"
        AccountManager.__init__(self, ledger_data_path)
        TransactionAccounter.__init__(self)

        self.ledger_entries : typing.List[LedgerEntry] = []
        
        self.__ledger_entries_file_path = ledger_data_path.joinpath("LedgerEntries.json")
        if not self.__ledger_entries_file_path.exists() :
            with utf8_file(self.__ledger_entries_file_path, 'x') as new_entry_file :
                new_entry_file.write("{\n")
                new_entry_file.write("\t\"entries\": []\n")
                new_entry_file.write("}")
        else :
            self.ledger_entries = json_read(self.__ledger_entries_file_path)["entries"]
            for entry in self.ledger_entries :
                self.account_transaction(entry.from_transaction.ID)
                self.account_transaction(entry.to_transaction.ID)

        self.__account_mapping_file_path = ledger_data_path.parent.joinpath(ledger_import.accounting_file + ".json")
        self.__initialize_category_tree()

        if not self.__account_mapping_file_path.exists() :
            with utf8_file(self.__account_mapping_file_path, 'x') as new_mapping_file :
                new_mapping_file.write("{\n")
                new_mapping_file.write("\t\"derived accounts\": [],\n")
                new_mapping_file.write("\t\"internal transactions\": []\n")
                new_mapping_file.write("}")

        for account_import in ledger_import.raw_accounts :
            self.__import_raw_account(ledger_data_path.parent, account_import)

        self.__clear()
        self.__derive_and_balance_accounts()
        self.__save()

    def __clear(self) :
        self.ledger_entries = []
        self.transaction_lookup = set()

    def __save(self) :
        json_write(self.__ledger_entries_file_path, {"entries" : self.ledger_entries})

    def __initialize_category_tree(self) :
        tree_nodes : typing.List[NameTreeNode] = json_read(self.__account_mapping_file_path)["derived account category tree"]
        self.category_tree = NameTree(tree_nodes)

        base_account_set = set(self.get_base_account_names())
        derived_account_set = set(self.get_derived_account_names())

        topological_sorter = TopologicalSorter(self.category_tree.node_dictionary)
        for node in topological_sorter.static_order() :
            assert node not in base_account_set, "Base accounts cannot be in the category tree!"
            derived_account_set.discard(node)

        assert len(derived_account_set) == 0, f"Not all derived accounts in tree! Missing ({derived_account_set})"

    def __import_raw_account(self, data_root : pathlib.Path, account_import : AccountImport) -> None :
        input_folder_path = data_root.joinpath(account_import.folder)
        if not input_folder_path.exists() :
            raise FileNotFoundError(f"Could not find expected filepath {input_folder_path}")

        account_name = input_folder_path.stem
        if self.account_is_created(account_name) :
            self.delete_account(account_name)

        read_transactions = read_transactions_from_csv_in_path(input_folder_path)
        if len(read_transactions.index) > 0 :
            read_transactions = read_transactions.sort_values(by=["timestamp"], kind="stable", ignore_index=True)
            read_transactions = make_identified_transaction_dataframe(read_transactions)
            self.create_account_from_transactions(self.base_account_data_path, False, account_name, read_transactions, account_import.opening_balance)

    def __make_ledger_entry(self, from_account_name, from_transaction_ID, to_account_name, to_transaction_ID, delta) :
        assert from_account_name != to_account_name, "Transaction to same account forbidden!"
        from_ledger_entry = LedgerTransaction(from_account_name, from_transaction_ID)
        to_ledger_entry = LedgerTransaction(to_account_name, to_transaction_ID)
        return LedgerEntry(from_ledger_entry, to_ledger_entry, abs(delta))

    def __validate_internal_account_mapping(self, from_account_name : str, from_matches : DataFrame, to_account_name : str, to_matches : DataFrame) -> None :
            
        #assumes in order on both accounts
        for (_, from_transaction), (_, to_transaction) in zip(from_matches.iterrows(), to_matches.iterrows()) :
            if from_transaction.delta != -to_transaction.delta :
                debug_message(f"Not in sync! Tried:\n\t{from_transaction.to_string()}\nTo:\n\t{to_transaction.to_string()}")
            else :
                entry = self.__make_ledger_entry(from_account_name, from_transaction.ID, to_account_name, to_transaction.ID, from_transaction.delta)
                self.ledger_entries.append(entry)
                self.account_transaction(entry.from_transaction.ID)
                self.account_transaction(entry.to_transaction.ID)
              
        #print missing transactions
        print_missed_transactions = lambda name, data : debug_message(f"\"{name}\" missing {len(data)} transactions:\n{data.to_csv()}")
        diff_from_to = len(to_matches) - len(from_matches)
        if diff_from_to < 0 :
            print_missed_transactions(to_account_name, from_matches.tail(-diff_from_to))
        elif diff_from_to > 0 :
            print_missed_transactions(from_account_name, to_matches.tail(diff_from_to))

        if len(from_matches) == 0 or len(to_matches) == 0 :
            debug_message("... nothing to map!")
        else :
            debug_message("... account mapped!")

    def __derive_and_balance_accounts(self) :

        #derived account matchings
        account_mapping_list : typing.List[DerivedAccount] = json_read(self.__account_mapping_file_path)["derived accounts"]
        for account_mapping in account_mapping_list :
            debug_message(f"Mapping spending account \"{account_mapping.name}\"")
            if self.account_is_created(account_mapping.name) :
                self.delete_account(account_mapping.name)

            deriver = AccountDeriver()

            if len(account_mapping.matchings) == 1 and account_mapping.matchings[0] is not None and account_mapping.matchings[0].account_name == "" :
                universal_match_strings = account_mapping.matchings[0].strings
                debug_message(f"Checking all base accounts for {universal_match_strings}")
                for account_name in self.get_base_account_names() :
                    deriver.check_account(self, account_name, universal_match_strings)
            else :
                for matching in account_mapping.matchings :
                    if matching.account_name == "" :
                        raise RuntimeError(f"Nonspecific match strings detected for account {account_mapping.name}! Not compatible with specified accounts!")
                    debug_message(f"Checking {matching.account_name} account for {matching.strings}")
                    deriver.check_account(self, matching.account_name, matching.strings)
            
            derived_transactions = deriver.get_matching_transactions()
            if len(derived_transactions) > 0 :
                account_name = account_mapping.name

                derived_transactions["order"] = range(len(derived_transactions))
                derived_transactions = derived_transactions.sort_values(by=["timestamp"], kind="stable", ignore_index=True)
                derived_transactions = make_identified_transaction_dataframe(derived_transactions)
                    
                derived_ledger_entries = []
                for (_, row) in derived_transactions.sort_values(by=["order"]).iterrows() :
                    derived_ledger_entries.append(self.__make_ledger_entry(row.source_account, row.source_ID, account_name, row.ID, row.delta))
                derived_transactions = derived_transactions[transaction_columns]
                
                try :
                    self.create_account_from_transactions(self.derived_account_data_path, True, account_name, derived_transactions, account_mapping.start_value)

                    for entry in derived_ledger_entries :
                        self.account_transaction(entry.from_transaction.ID)
                        self.account_transaction(entry.to_transaction.ID)
                    self.ledger_entries.extend(derived_ledger_entries)
                    
                    debug_message(f"... account {account_name} derived!")
                except Exception as e :
                    if self.account_is_created(account_name) :
                        self.delete_account(account_name)

                    debug_message(f"... exception {e} when {account_name} was derived!")
            else :
                debug_message(f"... nothing to map for {account_name}!")
            
            

        #internal transaction mappings
        transaction_mapping_list : typing.List[InternalTransactionMapping] = json_read(self.__account_mapping_file_path)["internal transactions"]
        for mapping in transaction_mapping_list :
            debug_message(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
            from_finder = AccountSearcher()
            from_finder.check_account(self, mapping.from_account, mapping.from_match_strings)
            to_finder = AccountSearcher()
            to_finder.check_account(self, mapping.to_account, mapping.to_match_strings)
            
            from_matching_transactions = from_finder.get_matching_transactions()
            to_matching_transactions = to_finder.get_matching_transactions()

            #check for double accounting
            for _, match in concat([from_matching_transactions, to_matching_transactions]).iterrows() :
                debug_assert(not self.transaction_accounted(match.ID), f"Transaction already accounted! : {match.to_string()}")

            self.__validate_internal_account_mapping(mapping.from_account, from_matching_transactions, mapping.to_account, to_matching_transactions)

    def get_unaccounted_transaction_table(self) -> DataFrame :
        unaccounted_transaction_list = []
        corresponding_account_list = []
        for account_name in self.get_account_names() :
            if not self.get_account_is_derived(account_name) :
                account_data = self.get_account_data(account_name)
                for (_, transaction) in account_data.transactions.iterrows() :
                    if not self.transaction_accounted(transaction.ID) :
                        unaccounted_transaction_list.append(transaction)
                        corresponding_account_list.append(account_name)
        unaccouted_table = DataFrame([{ "index" : idx, "date" : t.date, "description" : t.description, "delta" : t.delta } for idx, t in enumerate(unaccounted_transaction_list)])
        return unaccouted_table.join(Series(corresponding_account_list, name="Account"))
    
def open_ledger(ledger_path : pathlib.Path) -> typing.Optional[Ledger] :
    ledger_configuration = json_read(ledger_path.parent.joinpath("LedgerConfiguration.json"))

    for ledger_import in ledger_configuration.ledgers :
        if ledger_path.stem == ledger_import.name :
            assert ledger_path.exists()
            return Ledger(ledger_path, ledger_import)

    return None

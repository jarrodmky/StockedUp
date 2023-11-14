import typing
from pathlib import Path
from numpy import repeat, absolute, negative, not_equal, any

from pandas import DataFrame, Series, Index, concat

from PyJMy.json_file import json_read, json_write
from PyJMy.debug import debug_assert, debug_message
from PyJMy.utf8_file import utf8_file

from csv_importing import read_transactions_from_csv_in_path
from database import JsonDataBase

from accounting_objects import Account, UniqueHashCollector, make_hasher
from accounting_objects import LedgerImport, AccountImport, DerivedAccount, InternalTransactionMapping
from string_tree import StringTree

AccountList = typing.List[Account]

# raw account, display table, is derived
ManagedAccountData = typing.Tuple[Account, DataFrame, bool]

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
ledger_columns = ["from_account_name", "from_transaction_id", "to_account_name", "to_transaction_id", "delta"]

def make_identified_transaction_dataframe(transactions : DataFrame) -> DataFrame :
    make_id = lambda t : transaction_hash(int(t.name), t.date, t.timestamp, t.delta, t.description)
    transactions.insert(0, "ID", transactions.apply(make_id, axis=1, result_type="reduce"))
    return transactions

class LedgerDataBase :

    def __init__(self, root_path : Path, name : str) :
        self.__ledgerfile_path = root_path.joinpath(name)
        self.configuration_data = JsonDataBase(self.__ledgerfile_path, "Config")
        self.base_account_data = JsonDataBase(self.__ledgerfile_path, "BaseAccounts")
        self.derived_account_data = JsonDataBase(self.__ledgerfile_path, "DerivedAccounts")

    def retrieve_ledger_entries(self) -> DataFrame :
        if self.configuration_data.is_stored("LedgerEntries") :
            return DataFrame.from_records(self.configuration_data.retrieve("LedgerEntries")["entries"], columns=ledger_columns)
        else :
            new_ledger_entries = DataFrame(columns=ledger_columns)
            self.update_ledger_entries(new_ledger_entries)
            return new_ledger_entries

    def update_ledger_entries(self, ledger_entries : DataFrame) -> None :
        self.configuration_data.update("LedgerEntries", {"entries" : ledger_entries.to_dict("records")})


class AccountManager :

    def __init__(self, ledger_path : Path) :

        self.hash_register = UniqueHashCollector()
        self.database = LedgerDataBase(ledger_path.parent, ledger_path.stem)

        self.account_lookup : typing.Dict[str, ManagedAccountData] = {}
        for account in self.get_base_account_names() :
            acct_object = self.database.base_account_data.retrieve(account)
            assert acct_object is not None
            self.account_lookup[account] = make_managed_account(acct_object, False)

        for account in self.get_derived_account_names() :
            acct_object = self.database.derived_account_data.retrieve(account)
            assert acct_object is not None
            self.account_lookup[account] = make_managed_account(acct_object, True)

    def __get_account_data_pair(self, account_name : str) -> ManagedAccountData :
        assert self.account_is_created(account_name)
        return self.account_lookup[account_name]

    def get_account_names(self) -> typing.List[str] :
        return sorted(list(self.account_lookup.keys()))

    def get_base_account_names(self) -> typing.List[str] :
        return self.database.base_account_data.get_names()

    def get_derived_account_names(self) -> typing.List[str] :
        return self.database.derived_account_data.get_names()

    def account_is_created(self, account_name : str) -> bool :
        return account_name in self.account_lookup

    def get_account_data(self, account_name : str) -> Account :
        return self.__get_account_data_pair(account_name)[0]

    def get_account_table(self, account_name : str) -> DataFrame :
        return self.__get_account_data_pair(account_name)[1]

    def get_account_is_derived(self, account_name : str) -> bool :
        return self.__get_account_data_pair(account_name)[2]

    def delete_account(self, account_name : str) -> None :
        assert(self.account_is_created(account_name))

        if self.database.base_account_data.is_stored(account_name) :
            self.database.base_account_data.drop(account_name)

        if self.database.derived_account_data.is_stored(account_name) :
            self.database.derived_account_data.drop(account_name)

        del self.account_lookup[account_name]

class Accounter :

    def __init__(self) :
        self.transaction_lookup : typing.Set[int] = set()

    def transaction_accounted(self, transaction_ID : int) -> bool :
        return (transaction_ID in self.transaction_lookup)

    def account_transaction(self, transaction_ID : int) -> None :
        assert transaction_ID not in self.transaction_lookup, f"Duplicate unique hash already existing in ledger {transaction_ID}, likely double matched!"
        self.transaction_lookup.add(transaction_ID)

    def account_transactions(self, transaction_IDs : typing.List[int]) -> None :
        for id in transaction_IDs :
            self.account_transaction(id)

def escape_string(string : str) -> str :
    return string.replace("*", "\*").replace("+", "\+").replace("(", "\(").replace(")", "\)")

def strings_to_regex(strings : typing.List[str]) -> str :
    return "|".join([escape_string(s) for s in strings])
    
def get_matched_transactions(account_manager : AccountManager, account_name : str, string_matches : typing.List[str]) -> DataFrame :
    match_account = account_manager.get_account_data(account_name)
    debug_assert(match_account is not None, f"Account not found! Expected account \"{match_account.name}\" to exist!")
    debug_message(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
    
    matched_indices = match_account.transactions["description"].str.contains(strings_to_regex(string_matches))
    match_tuples = match_account.transactions[matched_indices]

    debug_message(f"Found {len(match_tuples)} transactions in {account_name}")
    return match_tuples

def derive_transaction_dataframe(account_name : str, dataframe : DataFrame) -> DataFrame :
    return DataFrame({
        "date" : dataframe.date,
        "delta" : -dataframe.delta,
        "description" : dataframe.description,
        "timestamp" : dataframe.timestamp,
        "source_ID" : dataframe.ID,
        "source_account" : repeat(account_name, len(dataframe))
    })

class Ledger(AccountManager, Accounter) :

    def __init__(self, ledger_data_path : Path, ledger_import : LedgerImport) :
        assert ledger_data_path.exists() or not ledger_data_path.is_dir(), "Expected ledger path not found!"
        AccountManager.__init__(self, ledger_data_path)
        Accounter.__init__(self)
        self.__account_mapping_file_path = ledger_data_path.parent.joinpath(ledger_import.accounting_file + ".json")

        if not self.__account_mapping_file_path.exists() :
            with utf8_file(self.__account_mapping_file_path, 'x') as new_mapping_file :
                new_mapping_file.write("{\n")
                new_mapping_file.write("\t\"derived accounts\": [],\n")
                new_mapping_file.write("\t\"internal transactions\": []\n")
                new_mapping_file.write("}")

        current_entries = self.database.retrieve_ledger_entries()
        self.account_transactions(current_entries.from_transaction_id)
        self.account_transactions(current_entries.to_transaction_id)

        for account_import in ledger_import.raw_accounts :
            self.__import_raw_account(ledger_data_path.parent, account_import)

        for account_mapping in self.__get_derived_account_mapping_list() :
            self.__derive_account(account_mapping)
        for mapping in self.__get_inter_account_mapping_list() :
            self.__map_account(mapping)
        self.category_tree = self.__make_category_tree()

    def __make_category_tree(self) -> StringTree :
        category_tree_dict = json_read(self.__account_mapping_file_path)["derived account category tree"]
        base_account_set = set(self.get_base_account_names())
        derived_account_set = set(self.get_derived_account_names())

        new_category_tree = StringTree(category_tree_dict, self.account_is_created)
        for node in new_category_tree.topological_sort() :
            assert node not in base_account_set, "Base accounts cannot be in the category tree!"
            derived_account_set.discard(node)

        assert len(derived_account_set) == 0, f"Not all derived accounts in tree! Missing ({derived_account_set})"

        return new_category_tree

    def __import_raw_account(self, data_root : Path, account_import : AccountImport) -> None :
        input_folder_path = data_root.joinpath(account_import.folder)
        if not input_folder_path.exists() :
            raise FileNotFoundError(f"Could not find expected filepath {input_folder_path}")

        account_name = input_folder_path.stem
        if self.account_is_created(account_name) :
            self.delete_account(account_name)

        assert not self.account_is_created(account_name), f"Account {account_name} already exists!"

        read_transactions = read_transactions_from_csv_in_path(input_folder_path)
        if len(read_transactions) > 0 :
            read_transactions.sort_values(by=["timestamp"], kind="stable", ignore_index=True, inplace=True)
            read_transactions = make_identified_transaction_dataframe(read_transactions)
            assert read_transactions.columns.equals(Index(transaction_columns))

            account = Account(self.hash_register, account_name, account_import.opening_balance, read_transactions)
            self.database.base_account_data.store(account_name, account)
            self.account_lookup[account.name] = make_managed_account(account, False)

    def __validate_internal_account_mapping(self, from_account_name : str, from_matches : DataFrame, to_account_name : str, to_matches : DataFrame) -> None :
            
        assert from_account_name != to_account_name, "Transaction to same account forbidden!"
        matched_length = min(len(from_matches.index), len(to_matches.index))

        #assumes in order on both accounts
        from_matches_trunc = from_matches.head(matched_length)
        to_matches_trunc = to_matches.head(matched_length)
        if any(not_equal(from_matches_trunc.delta.values, negative(to_matches_trunc.delta.values))) :
            debug_message(f"Not in sync! Tried:\n\t{from_account_name}\nTo:\n\t{to_account_name}")
        
        internal_ledger_entries = DataFrame({
            "from_account_name" : repeat(from_account_name, matched_length),
            "from_transaction_id" : from_matches_trunc.ID.values,
            "to_account_name" : repeat(to_account_name, matched_length),
            "to_transaction_id" : to_matches_trunc.ID.values,
            "delta" : absolute(from_matches_trunc.delta.values)
        })
        self.__account_ledger_entries(internal_ledger_entries)
              
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

    def __get_derived_account_mapping_list(self) -> typing.List[DerivedAccount] :
        return json_read(self.__account_mapping_file_path)["derived accounts"]

    def __get_inter_account_mapping_list(self) -> typing.List[InternalTransactionMapping] :
        return json_read(self.__account_mapping_file_path)["internal transactions"]
    
    def __account_ledger_entries(self, new_entries : DataFrame) :
        assert new_entries.columns.equals(Index(ledger_columns)), "Incompatible columns detected!"
        if len(new_entries) > 0 :
            self.account_transactions(new_entries.from_transaction_id)
            self.account_transactions(new_entries.to_transaction_id)
            current_entries : DataFrame = self.database.retrieve_ledger_entries()
            self.database.update_ledger_entries(concat([current_entries, new_entries]))

    def __derive_account(self, account_mapping : DerivedAccount) -> None :

        debug_message(f"Mapping spending account \"{account_mapping.name}\"")
        if self.account_is_created(account_mapping.name) :
            self.delete_account(account_mapping.name)

        derived_transaction_frames = []

        if len(account_mapping.matchings) == 1 and account_mapping.matchings[0] is not None and account_mapping.matchings[0].account_name == "" :
            universal_match_strings = account_mapping.matchings[0].strings
            debug_message(f"Checking all base accounts for {universal_match_strings}")
            for account_name in self.get_base_account_names() :
                found_tuples = get_matched_transactions(self, account_name, universal_match_strings)
                derived_transaction_frames.append(derive_transaction_dataframe(account_name, found_tuples))
                
        else :
            for matching in account_mapping.matchings :
                if matching.account_name == "" :
                    raise RuntimeError(f"Nonspecific match strings detected for account {account_mapping.name}! Not compatible with specified accounts!")
                debug_message(f"Checking {matching.account_name} account for {matching.strings}")
                found_tuples = get_matched_transactions(self, matching.account_name, matching.strings)
                derived_transaction_frames.append(derive_transaction_dataframe(matching.account_name, found_tuples))
        
        derived_transactions = concat(derived_transaction_frames, ignore_index=True)
        derived_transactions.sort_values(by=["timestamp"], kind="stable", ignore_index=True, inplace=True)
        if len(derived_transactions) > 0 :
            account_name = account_mapping.name
            assert account_name not in derived_transactions.source_account.unique(), "Transaction to same account forbidden!"

            derived_transactions = make_identified_transaction_dataframe(derived_transactions)

            try :
                derived_ledger_entries = DataFrame({
                    "from_account_name" : derived_transactions.source_account,
                    "from_transaction_id" : derived_transactions.source_ID,
                    "to_account_name" : repeat(account_name, len(derived_transactions.index)),
                    "to_transaction_id" : derived_transactions.ID,
                    "delta" : absolute(derived_transactions.delta)
                })
                self.__account_ledger_entries(derived_ledger_entries)

                derived_transactions = derived_transactions[transaction_columns]
                account = Account(self.hash_register, account_name, account_mapping.start_value, derived_transactions)
                self.database.derived_account_data.store(account_name, account)
                self.account_lookup[account.name] = make_managed_account(account, True)
                
                debug_message(f"... account {account_name} derived!")
            except Exception as e :
                if self.account_is_created(account_name) :
                    self.delete_account(account_name)

                debug_message(f"... exception {e} when {account_name} was derived!")
        else :
            debug_message(f"... nothing to map for {account_name}!")

    def __map_account(self, mapping : InternalTransactionMapping) -> None :
            
        #internal transaction mappings
        debug_message(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
        
        from_matching_transactions = get_matched_transactions(self, mapping.from_account, mapping.from_match_strings)
        to_matching_transactions = get_matched_transactions(self, mapping.to_account, mapping.to_match_strings)

        #check for double accounting
        for _, matched_id in concat([from_matching_transactions.ID, to_matching_transactions.ID]).items() :
            debug_assert(not self.transaction_accounted(matched_id), f"Transaction already accounted! : {matched_id}")

        self.__validate_internal_account_mapping(mapping.from_account, from_matching_transactions, mapping.to_account, to_matching_transactions)

    def get_unaccounted_transaction_table(self) -> DataFrame :
        unaccounted_transaction_list = []
        corresponding_account_list = []
        for account_name in self.get_base_account_names() :
            account_data = self.get_account_data(account_name)
            for (_, transaction) in account_data.transactions.iterrows() :
                if not self.transaction_accounted(transaction.ID) :
                    unaccounted_transaction_list.append(transaction)
                    corresponding_account_list.append(account_name)
        unaccouted_table = DataFrame([{ "index" : idx, "date" : t.date, "description" : t.description, "delta" : t.delta } for idx, t in enumerate(unaccounted_transaction_list)])
        return unaccouted_table.join(Series(corresponding_account_list, name="Account"))

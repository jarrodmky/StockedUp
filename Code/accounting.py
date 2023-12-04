import typing
from pathlib import Path
from numpy import repeat, absolute, negative, not_equal, any

from pandas import DataFrame, Series, Index, concat

from PyJMy.json_file import json_read
from PyJMy.debug import debug_assert, debug_message
from PyJMy.utf8_file import utf8_file

from accounting_objects import Account
from accounting_objects import LedgerImport, DerivedAccount, InternalTransactionMapping
from string_tree import StringTree
from ledger_database import LedgerDataBase, UniqueHashCollector, ledger_columns, transaction_columns, make_identified_transaction_dataframe, managed_account_data_hash

def escape_string(string : str) -> str :
    return string.replace("*", "\*").replace("+", "\+").replace("(", "\(").replace(")", "\)")

def strings_to_regex(strings : typing.List[str]) -> str :
    return "|".join([escape_string(s) for s in strings])
    
def get_matched_transactions(database : LedgerDataBase, account_name : str, string_matches : typing.List[str]) -> DataFrame :
    match_account = database.get_account(account_name)
    debug_assert(match_account is not None, f"Account not found! Expected account \"{match_account.name}\" to exist!")
    debug_message(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
    
    matched_indices = match_account.transactions["description"].str.contains(strings_to_regex(string_matches))
    match_tuples = match_account.transactions[matched_indices]

    debug_message(f"Found {len(match_tuples)} transactions in {account_name}")
    return match_tuples

def get_derived_matched_transactions(database : LedgerDataBase, derived_account_mapping : DerivedAccount) -> DataFrame :
    matched_transaction_frames = []

    if len(derived_account_mapping.matchings) == 1 and derived_account_mapping.matchings[0] is not None and derived_account_mapping.matchings[0].account_name == "" :
        universal_match_strings = derived_account_mapping.matchings[0].strings
        debug_message(f"Checking all base accounts for {universal_match_strings}")
        for account_name in database.get_source_names() :
            found_tuples = get_matched_transactions(database, account_name, universal_match_strings)
            matched_transaction_frames.append(derive_transaction_dataframe(account_name, found_tuples))
            
    else :
        for matching in derived_account_mapping.matchings :
            if matching.account_name == "" :
                raise RuntimeError(f"Nonspecific match strings detected for account {derived_account_mapping.name}! Not compatible with specified accounts!")
            debug_message(f"Checking {matching.account_name} account for {matching.strings}")
            found_tuples = get_matched_transactions(database, matching.account_name, matching.strings)
            matched_transaction_frames.append(derive_transaction_dataframe(matching.account_name, found_tuples))
    
    all_matched_transactions = concat(matched_transaction_frames, ignore_index=True)
    all_matched_transactions.sort_values(by=["timestamp"], kind="stable", ignore_index=True, inplace=True)
    return all_matched_transactions


def derive_transaction_dataframe(account_name : str, dataframe : DataFrame) -> DataFrame :
    return DataFrame({
        "date" : dataframe.date,
        "delta" : -dataframe.delta,
        "description" : dataframe.description,
        "timestamp" : dataframe.timestamp,
        "source_ID" : dataframe.ID,
        "source_account" : repeat(account_name, len(dataframe))
    })

class Ledger :

    def __init__(self, ledger_data_path : Path, ledger_import : LedgerImport) :
        assert ledger_data_path.exists() or not ledger_data_path.is_dir(), "Expected ledger path not found!"
        self.hash_register = UniqueHashCollector()
        self.__account_mapping_file_path = ledger_data_path.parent / (ledger_import.accounting_file + ".json")

        self.database = LedgerDataBase(self.hash_register, ledger_data_path.parent, ledger_data_path.stem, ledger_import.raw_accounts)
        self.__transaction_lookup : typing.Set[int] = set()

        if not self.__account_mapping_file_path.exists() :
            with utf8_file(self.__account_mapping_file_path, 'x') as new_mapping_file :
                new_mapping_file.write("{\n")
                new_mapping_file.write("\t\"derived accounts\": [],\n")
                new_mapping_file.write("\t\"internal transactions\": []\n")
                new_mapping_file.write("}")

        current_entries = self.database.retrieve_ledger_entries()
        self.__account_transactions(current_entries.from_transaction_id)
        self.__account_transactions(current_entries.to_transaction_id)

        for account_mapping in self.__get_derived_account_mapping_list() :
            self.__derive_account(account_mapping)
        for mapping in self.__get_inter_account_mapping_list() :
            self.__map_account(mapping)
        self.category_tree = self.__make_category_tree()

    def __transaction_accounted(self, id : int) -> bool :
        return (id in self.__transaction_lookup)

    def __account_transactions(self, new_ids : Series) -> None :
        new_id_set = set(new_ids)
        assert new_id_set not in self.__transaction_lookup, f"Duplicate unique hashes already existing in ledger:\n{list(new_ids - self.__transaction_lookup)}\n, likely double matched!"
        for id in new_ids :
            self.__transaction_lookup.add(id)

    def __make_category_tree(self) -> StringTree :
        category_tree_dict = json_read(self.__account_mapping_file_path)["derived account category tree"]
        base_account_set = set(self.database.get_source_names())
        derived_account_set = set(self.get_derived_account_names())

        new_category_tree = StringTree(category_tree_dict, self.database.account_is_created)
        for node in new_category_tree.topological_sort() :
            assert node not in base_account_set, "Base accounts cannot be in the category tree!"
            derived_account_set.discard(node)

        assert len(derived_account_set) == 0, f"Not all derived accounts in tree! Missing ({derived_account_set})"

        return new_category_tree

    def __get_derived_account_mapping_list(self) -> typing.List[DerivedAccount] :
        return json_read(self.__account_mapping_file_path)["derived accounts"]

    def __get_inter_account_mapping_list(self) -> typing.List[InternalTransactionMapping] :
        return json_read(self.__account_mapping_file_path)["internal transactions"]
    
    def __account_ledger_entries(self, new_entries : DataFrame) -> None :
        assert new_entries.columns.equals(Index(ledger_columns)), "Incompatible columns detected!"
        if len(new_entries) > 0 :
            self.__account_transactions(new_entries.from_transaction_id)
            self.__account_transactions(new_entries.to_transaction_id)
            current_entries : DataFrame = self.database.retrieve_ledger_entries()
            self.database.update_ledger_entries(concat([current_entries, new_entries]))

    def __derive_account(self, account_mapping : DerivedAccount) -> None :

        account_name = account_mapping.name
        if self.database.derived_account_data.is_stored(account_name) :
            debug_message(f"Skip deriving account {account_name}, already exists!")
            return
        
        debug_message(f"Mapping spending account \"{account_name}\"")
        derived_transactions = get_derived_matched_transactions(self.database, account_mapping)
        if len(derived_transactions) > 0 :
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

                account = Account(account_name, account_mapping.start_value, derived_transactions)
                account.ID = managed_account_data_hash(self.hash_register, account)
                self.hash_register.register_hash("Account", account.ID, f"Acct={account.name}")

                self.database.create_derived_account(account_name, account)
                
                debug_message(f"... account {account_name} derived!")
            except Exception as e :
                if self.database.derived_account_data.is_stored(account_name) :
                    self.database.derived_account_data.drop(account_name)

                debug_message(f"... exception {e} when {account_name} was derived!")
        else :
            debug_message(f"... nothing to map for {account_name}!")

    def __map_account(self, mapping : InternalTransactionMapping) -> None :
            
        #internal transaction mappings
        debug_message(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
        
        from_matching_transactions = get_matched_transactions(self.database, mapping.from_account, mapping.from_match_strings)
        to_matching_transactions = get_matched_transactions(self.database, mapping.to_account, mapping.to_match_strings)

        #check for double accounting
        for _, matched_id in concat([from_matching_transactions.ID, to_matching_transactions.ID]).items() :
            debug_assert(not self.__transaction_accounted(matched_id), f"Transaction already accounted! : {matched_id}")
        
        from_account_name = mapping.from_account
        to_account_name = mapping.to_account
            
        assert from_account_name != to_account_name, "Transaction to same account forbidden!"
        matched_length = min(len(from_matching_transactions.index), len(to_matching_transactions))

        #assumes in order on both accounts
        from_matches_trunc = from_matching_transactions.head(matched_length)
        to_matches_trunc = to_matching_transactions.head(matched_length)
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
        diff_from_to = len(to_matching_transactions) - len(from_matching_transactions)
        if diff_from_to < 0 :
            print_missed_transactions(to_account_name, from_matching_transactions.tail(-diff_from_to))
        elif diff_from_to > 0 :
            print_missed_transactions(from_account_name, to_matching_transactions.tail(diff_from_to))

        if len(from_matching_transactions) == 0 or len(to_matching_transactions) == 0 :
            debug_message("... nothing to map!")
        else :
            debug_message("... account mapped!")

    def get_unaccounted_transaction_table(self) -> DataFrame :
        unaccounted_transaction_list = []
        corresponding_account_list = []
        for account_name in self.database.get_source_names() :
            account_data = self.database.get_account(account_name)
            for (_, transaction) in account_data.transactions.iterrows() :
                if not self.__transaction_accounted(transaction.ID) :
                    unaccounted_transaction_list.append(transaction)
                    corresponding_account_list.append(account_name)
        dataframe = DataFrame([{ "index" : idx, "date" : t.date, "description" : t.description, "delta" : t.delta } for idx, t in enumerate(unaccounted_transaction_list)])
        dataframe["account"] = corresponding_account_list
        return dataframe

    def get_base_account_names(self) -> typing.List[str] :
        return self.database.get_source_names()

    def get_derived_account_names(self) -> typing.List[str] :
        return self.database.derived_account_data.get_names()

import typing
from pathlib import Path
from numpy import repeat, absolute, negative, not_equal, any
from pandas import DataFrame, Series, Index, concat

from PyJMy.json_file import json_read
from PyJMy.debug import debug_assert, debug_message
from PyJMy.utf8_file import utf8_file

from accounting_objects import LedgerImport, InternalTransactionMapping
from string_tree import StringTree
from ledger_database import LedgerDataBase, UniqueHashCollector, ledger_columns, get_matched_transactions, LedgerEntryFrame

class Ledger :

    def __init__(self, ledger_data_path : Path, ledger_import : LedgerImport) :
        assert ledger_data_path.exists() or not ledger_data_path.is_dir(), "Expected ledger path not found!"
        self.hash_register = UniqueHashCollector()
        self.__account_mapping_file_path = ledger_data_path.parent / (ledger_import.accounting_file + ".json")

        self.__transaction_lookup : typing.Set[int] = set()
        self.database = LedgerDataBase(self.hash_register, ledger_data_path.parent, ledger_data_path.stem, lambda df, le : self.__account_ledger_entries(df, le))

        if not self.__account_mapping_file_path.exists() :
            with utf8_file(self.__account_mapping_file_path, 'x') as new_mapping_file :
                new_mapping_file.write("{\n")
                new_mapping_file.write("\t\"derived accounts\": [],\n")
                new_mapping_file.write("\t\"internal transactions\": []\n")
                new_mapping_file.write("}")

        current_entries = self.database.ledger_entries.retrieve()
        self.__account_transactions(current_entries.from_transaction_id)
        self.__account_transactions(current_entries.to_transaction_id)

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
        base_account_set = set(self.database.get_source_account_names())
        derived_account_set = set(self.database.get_derived_account_names())

        new_category_tree = StringTree(category_tree_dict, self.database.account_is_created)
        for node in new_category_tree.topological_sort() :
            assert node not in base_account_set, "Base accounts cannot be in the category tree!"
            derived_account_set.discard(node)

        assert len(derived_account_set) == 0, f"Not all derived accounts in tree! Missing ({derived_account_set})"

        return new_category_tree

    def __get_inter_account_mapping_list(self) -> typing.List[InternalTransactionMapping] :
        return json_read(self.__account_mapping_file_path)["internal transactions"]
    
    def __account_ledger_entries(self, new_entries : DataFrame, ledger_entries : LedgerEntryFrame) -> None :
        assert new_entries.columns.equals(Index(ledger_columns)), "Incompatible columns detected!"
        if len(new_entries) > 0 :
            self.__account_transactions(new_entries.from_transaction_id)
            self.__account_transactions(new_entries.to_transaction_id)
            current_entries : DataFrame = ledger_entries.retrieve()
            ledger_entries.update(concat([current_entries, new_entries]))

    def __map_account(self, mapping : InternalTransactionMapping) -> None :
            
        #internal transaction mappings
        debug_message(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
        
        from_account_name = mapping.from_account
        to_account_name = mapping.to_account

        from_account = self.database.get_account(from_account_name)
        to_account = self.database.get_account(to_account_name)

        from_matching_transactions = get_matched_transactions(from_account, mapping.from_match_strings)
        to_matching_transactions = get_matched_transactions(to_account, mapping.to_match_strings)

        #check for double accounting
        for _, matched_id in concat([from_matching_transactions.ID, to_matching_transactions.ID]).items() :
            debug_assert(not self.__transaction_accounted(matched_id), f"Transaction already accounted! : {matched_id}")
        
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
        self.__account_ledger_entries(internal_ledger_entries, self.database.ledger_entries)
              
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
        for account_data in self.database.get_source_accounts() :
            for (_, transaction) in account_data.transactions.iterrows() :
                if not self.__transaction_accounted(transaction.ID) :
                    unaccounted_transaction_list.append(transaction)
                    corresponding_account_list.append(account_data.name)
        dataframe = DataFrame([{ "index" : idx, "date" : t.date, "description" : t.description, "delta" : t.delta } for idx, t in enumerate(unaccounted_transaction_list)])
        dataframe["account"] = corresponding_account_list
        return dataframe

import typing
from pathlib import Path
from numpy import repeat
from polars import DataFrame, Series, concat, Float64, String

from Code.PyJMy.json_file import json_read
from Code.PyJMy.utf8_file import utf8_file

from Code.logger import get_logger
logger = get_logger(__name__)

from Code.Data.account_data import ledger_columns, Account
from Code.Data.hashing import UniqueHashCollector

from Code.accounting_objects import LedgerImport, InternalTransactionMapping
from Code.string_tree import StringTree, StringDict
from Code.ledger_database import LedgerDataBase, get_matched_transactions, LedgerEntryFrame

AccountCache = typing.Dict[str, Account]

def make_category_tree(ledger_db : LedgerDataBase, category_tree_dict : StringDict) -> StringTree :
    base_account_set = set(ledger_db.get_source_account_names())
    derived_account_set = set(ledger_db.get_derived_account_names())
    new_category_tree = StringTree(category_tree_dict, ledger_db.account_is_created)
    for node in new_category_tree.topological_sort() :
        assert node not in base_account_set, "Base accounts cannot be in the category tree!"
        derived_account_set.discard(node)
    assert len(derived_account_set) == 0, f"Not all derived accounts in tree! Missing ({derived_account_set})"
    return new_category_tree 

def verify_account_correspondence(accounted_transaction_set : typing.Set[str], account_cache : AccountCache, mapping : InternalTransactionMapping) -> DataFrame :
    from_account = account_cache[mapping.from_account]
    to_account = account_cache[mapping.to_account]
    
    from_matching_transactions = get_matched_transactions(from_account, mapping.from_match_strings)
    to_matching_transactions = get_matched_transactions(to_account, mapping.to_match_strings)

    #check for double accounting
    for matched_id in concat([from_matching_transactions["ID"], to_matching_transactions["ID"]]) :
        if (matched_id in accounted_transaction_set) :
            logger.error(f"Transaction already accounted! : {matched_id}")

    from_account_name = from_account.name
    to_account_name = to_account.name

    #assumes in order on both accounts
    matched_length = min(from_matching_transactions.height, to_matching_transactions.height)
    from_matches_trunc = from_matching_transactions.head(matched_length)
    to_matches_trunc = to_matching_transactions.head(matched_length)
    if not from_matches_trunc["delta"].equals(-to_matches_trunc["delta"], strict=True) :
        logger.info(f"Not in sync! Tried:\n\t{from_account_name}\nTo:\n\t{to_account_name}")
          
    #print missing transactions
    print_missed_transactions = lambda name, data : logger.info(f"\"{name}\" missing {len(data)} transactions:\n{data.write_csv()}")
    diff_from_to = to_matching_transactions.height - from_matching_transactions.height
    if diff_from_to < 0 :
        print_missed_transactions(to_account_name, from_matching_transactions.tail(-diff_from_to))
    elif diff_from_to > 0 :
        print_missed_transactions(from_account_name, to_matching_transactions.tail(diff_from_to))
    if from_matching_transactions.height == 0 or to_matching_transactions.height == 0 :
        logger.info("... nothing to map!")
    else :
        logger.info("... account mapped!")
    
    internal_ledger_entries = DataFrame({
        "from_account_name" : repeat(from_account_name, matched_length),
        "from_transaction_id" : from_matches_trunc["ID"],
        "to_account_name" : repeat(to_account_name, matched_length),
        "to_transaction_id" : to_matches_trunc["ID"],
        "delta" : from_matches_trunc["delta"].abs()
    })
    return internal_ledger_entries

class Ledger :

    def __init__(self, ledger_data_path : Path, ledger_import : LedgerImport) :
        assert ledger_data_path.exists() or not ledger_data_path.is_dir(), "Expected ledger path not found!"
        self.hash_register = UniqueHashCollector()
        self.__account_mapping_file_path = ledger_data_path.parent / (ledger_import.accounting_file + ".json")

        self.__transaction_lookup : typing.Set[str] = set()
        self.__database = LedgerDataBase(self.hash_register, ledger_data_path.parent, ledger_data_path.stem, self.__account_ledger_entries)

        if not self.__account_mapping_file_path.exists() :
            with utf8_file(self.__account_mapping_file_path, 'x') as new_mapping_file :
                new_mapping_file.write("{\n")
                new_mapping_file.write("\t\"derived accounts\": [],\n")
                new_mapping_file.write("\t\"internal transactions\": []\n")
                new_mapping_file.write("}")

        #Get stored ledger entries (TODO this makes data unable to build twice)
        current_entries = self.__database.ledger_entries.retrieve()
        new_ids = concat([current_entries["from_transaction_id"], current_entries["to_transaction_id"]])
        assert set(new_ids) not in self.__transaction_lookup, f"Duplicate unique hashes already existing in ledger:\n{list(new_ids - self.__transaction_lookup)}\n, likely double matched!"
        for id in new_ids :
            self.__transaction_lookup.add(id)

        internal_transactions = json_read(self.__account_mapping_file_path)["internal transactions"]

        account_cache = {}
        for mapping in internal_transactions :
            if mapping.from_account not in account_cache :
                account_cache[mapping.from_account] = self.__database.get_account(mapping.from_account)
            if mapping.to_account not in account_cache :
                account_cache[mapping.to_account] = self.__database.get_account(mapping.to_account)

        for mapping in internal_transactions :
            #internal transaction mappings
            if mapping.from_account != mapping.to_account :
                logger.info(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
                new_ledger_entries = verify_account_correspondence(self.__transaction_lookup, account_cache, mapping)
        
                self.__account_ledger_entries(new_ledger_entries, self.__database.ledger_entries)
            else :
                logger.error(f"Transactions to same account {mapping.from_account}?")

        category_tree_dict = json_read(self.__account_mapping_file_path)["derived account category tree"]
        self.category_tree = make_category_tree(self.__database, category_tree_dict)

    def get_account(self, account_name : str) -> Account :
        return self.__database.get_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__database.get_source_account_names()
    
    def __account_ledger_entries(self, new_entries : DataFrame, ledger_entries : LedgerEntryFrame) -> None :
        assert new_entries.columns == ledger_columns, "Incompatible columns detected!"
        if len(new_entries) > 0 :
            new_ids = concat([new_entries["from_transaction_id"], new_entries["to_transaction_id"]])
            assert set(new_ids) not in self.__transaction_lookup, f"Duplicate unique hashes already existing in ledger:\n{list(new_ids - self.__transaction_lookup)}\n, likely double matched!"
            for id in new_ids :
                self.__transaction_lookup.add(id)
            current_entries : DataFrame = ledger_entries.retrieve()
            ledger_entries.update(concat([current_entries, new_entries]))

    def get_unaccounted_transaction_table(self) -> DataFrame :
        empty_frame = DataFrame(schema={
            "date" : String,
            "description" : String,
            "delta" : Float64,
            "account" : String
            })
        unaccounted_transactions_data_frame_list = [empty_frame]
        accounted_transactions = DataFrame(Series("ID", list(self.__transaction_lookup)))
        for account_data in self.__database.get_source_accounts() :
            unaccounted_dataframe = (account_data.transactions
                .join(accounted_transactions, "ID", "anti")
                .select(["date", "description", "delta"]))
            account_column = Series("account", repeat(account_data.name, unaccounted_dataframe.height))
            unaccounted_dataframe = unaccounted_dataframe.insert_column(unaccounted_dataframe.width, account_column)
            unaccounted_transactions_data_frame_list.append(unaccounted_dataframe)
        unaccounted_transactions = concat(unaccounted_transactions_data_frame_list)
        return unaccounted_transactions.insert_column(0, Series("index", range(0, unaccounted_transactions.height)))

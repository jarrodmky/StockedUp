import typing
from pathlib import Path

from Code.logger import get_logger
logger = get_logger(__name__)

from Code.Data.account_data import Account, LedgerImport

from Code.json_utils import json_serializer
from Code.string_tree import StringTree, StringDict
from Code.ledger_database import LedgerDataBase

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

class Ledger :

    def __init__(self, ledger_data_path : Path, ledger_import : LedgerImport) :
        assert ledger_data_path.exists() or not ledger_data_path.is_dir(), "Expected ledger path not found!"

        self.__database = LedgerDataBase(ledger_data_path.parent, ledger_data_path.stem, ledger_import.accounting_file)

        logger.info(f"Database created for {ledger_data_path.stem}")
        account_mapping_file_path = ledger_data_path.parent / (ledger_import.accounting_file + ".json")
        category_tree_dict = {}
        if account_mapping_file_path.exists() :
            category_tree_dict = json_serializer.read_from_file(account_mapping_file_path)["derived account category tree"]
        self.category_tree = make_category_tree(self.__database, category_tree_dict)

    def get_account(self, account_name : str) -> Account :
        return self.__database.get_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__database.get_source_account_names()

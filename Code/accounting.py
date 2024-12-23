import typing
from pathlib import Path
from polars import DataFrame

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

from Code.Data.account_data import Account, LedgerImport, AccountMapping

from Code.Utils.json_serializer import json_serializer
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

    def __init__(self, data_root_directory : Path, ledger_import : LedgerImport) :
        ledger_data_path = data_root_directory.joinpath(ledger_import.ledger_name)
        if not ledger_data_path.exists() :
            logger.info(f"Creating ledger folder {ledger_data_path}")
            ledger_data_path.mkdir()

        assert ledger_data_path.exists() or not ledger_data_path.is_dir(), "Expected ledger path not found!"

        account_mapping_file_path = data_root_directory / (ledger_import.accounting_file + ".json")
        if not account_mapping_file_path.exists() :
            json_serializer.write_to_file(account_mapping_file_path, AccountMapping())
            account_mapping = AccountMapping()
        else :
            account_mapping = json_serializer.read_from_file(account_mapping_file_path, AccountMapping)

        self.__database = LedgerDataBase(data_root_directory, ledger_import, account_mapping)
        logger.info(f"Database created for {ledger_import.ledger_name}")

        account_mapping_file_path = data_root_directory / (ledger_import.accounting_file + ".json")
        category_tree_dict = {}
        if account_mapping_file_path.exists() :
            category_tree_dict = json_serializer.read_from_file(account_mapping_file_path)["derived account category tree"]
        self.category_tree = make_category_tree(self.__database, category_tree_dict)

    def get_account(self, account_name : str) -> Account :
        return self.__database.get_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__database.get_source_account_names()
    
    def get_unaccounted_transaction_table(self) -> DataFrame :
        return self.__database.get_unaccounted_transaction_table()

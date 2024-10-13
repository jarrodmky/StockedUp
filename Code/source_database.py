import typing
from pathlib import Path

from Code.database import JsonDataBase
from Code.object_cacher import ObjectCacher
from Code.Data.account_data import Account, AccountImport
from Code.Pipeline.account_importing import get_imported_account, get_imported_account_hash

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

class SourceDataBase(JsonDataBase) :
    
    def __init__(self, hash_db : JsonDataBase, ledger_output_path : Path, account_imports : typing.List[AccountImport], account_data_path : Path) :
        super().__init__(ledger_output_path, "BaseAccounts")
        self.__cache = ObjectCacher(hash_db, "ImportedAccountHashes", Account())
        self.__account_data_path = account_data_path
        self.__import_data_lookup = {}

        for account_import in account_imports :
            self.__import_data_lookup[account_import.account_name] = account_import
        for account_import in account_imports :
            self.get_account(account_import.account_name)

    def __import_account(self, account_name : str) -> Account | None :
        account_import = self.__import_data_lookup[account_name]
        account = get_imported_account(self.__account_data_path, account_import)
        logger.info(f"Imported account {account_name}!")
        return account
    
    def get_account_hash(self, account_name : str) -> str :
        account_import = self.__import_data_lookup[account_name]
        return get_imported_account_hash(self.__account_data_path, account_import)

    def get_account(self, account_name : str) -> Account :
        if account_name not in self.__import_data_lookup :
            logger.info(f"Account {account_name} not found in import data!")
            return Account()

        current_hash = self.get_account_hash(account_name)
        return self.__cache.request_object(self, account_name, current_hash, self.__import_account)

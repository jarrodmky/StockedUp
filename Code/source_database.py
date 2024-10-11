import typing
from pathlib import Path

from Code.database import JsonDataBase
from Code.hash_checker import HashChecker
from Code.Data.account_data import Account, AccountImport
from Code.Pipeline.account_importing import get_imported_account, get_imported_account_hash

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

class SourceDataBase(JsonDataBase) :
    
    def __init__(self, hash_db : JsonDataBase, ledger_output_path : Path, account_imports : typing.List[AccountImport], account_data_path : Path) :
        super().__init__(ledger_output_path, "BaseAccounts")
        self.__hash_checker = HashChecker(hash_db, "ImportedAccountHashes")
        self.__account_data_path = account_data_path
        self.__import_data_lookup = {}

        for account_import in account_imports :
            self.__import_data_lookup[account_import.account_name] = account_import
        for account_import in account_imports :
            self.get_account(account_import.account_name)

    def __import_account(self, account_name : str) -> Account | None :
        account_import = self.__import_data_lookup[account_name]
        try :
            account = get_imported_account(self.__account_data_path, account_import)
            logger.info(f"Imported account {account_name}!")
            return account
        except Exception as e :
            logger.error(f"Failed to import account {account_name}! {e}")
        return None
    
    def get_account_hash(self, account_name : str) -> str :
        account_import = self.__import_data_lookup[account_name]
        return get_imported_account_hash(self.__account_data_path, account_import)

    def get_account(self, account_name : str) -> Account :
        if account_name not in self.__import_data_lookup :
            logger.info(f"Account {account_name} not found in import data!")
            return Account()
        
        result_hash = self.get_account_hash(account_name)
        stored_hash = self.__hash_checker.get_stored_hash(account_name)
        if stored_hash == result_hash :
            #hash same, no action
            return self.retrieve(account_name, Account)
        
        account = self.__import_account(account_name)
        if isinstance(account, Account) :
            self.__hash_checker.set_stored_hash(account_name, result_hash)
            self.update(account_name, account)
            return account
        logger.warning(f"Failed to find account {account_name}, returning default")
        return Account(account_name)

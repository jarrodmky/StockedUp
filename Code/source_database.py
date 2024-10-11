import typing
from pathlib import Path

from Code.database import JsonDataBase
from Code.Data.account_data import Account, AccountImport
from Code.Pipeline.account_importing import get_imported_account, get_imported_account_hash

from Code.logger import get_logger
logger = get_logger(__name__)

class HashChecker :

    def __init__(self, hash_db : JsonDataBase, hash_object_name : str) :
        self.__hash_db = hash_db
        self.__hash_object_name = hash_object_name
        
    def __get_stored_hashes(self) :
        if self.__hash_db.is_stored(self.__hash_object_name) :
            return self.__hash_db.retrieve(self.__hash_object_name)
        else :
            self.__hash_db.store(self.__hash_object_name, {})
            return {}

    def get_stored_hash(self, name : str) -> str :
        source_hashes = self.__get_stored_hashes()
        if name in source_hashes :
            stored_hash = source_hashes[name]
            assert stored_hash != 0, "Stored 0 hashes forbidden, means import never done or invalid!"
            return stored_hash
        else :
            return "0"

    def set_stored_hash(self, name : str, new_hash : str) -> None :
        assert self.get_stored_hash(name) != new_hash, "Setting new hash without checking it?"
        source_hashes = self.__get_stored_hashes()
        if new_hash != 0 :
            source_hashes[name] = new_hash
        else :
            if name in source_hashes :
                del source_hashes[name]
            else :
                logger.info("Zeroing out hash, something destructive or erroneous happened!")
        self.__hash_db.update(self.__hash_object_name, source_hashes)

class SourceDataBase :
    
    def __init__(self, hash_db : JsonDataBase, ledger_output_path : Path, account_imports : typing.List[AccountImport], account_data_path : Path) :
        self.__db = JsonDataBase(ledger_output_path, "BaseAccounts")
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
            return account
        except Exception as e :
            logger.info(f"Failed to import account {account_name}! {e}")
        return None
    
    def get_account_hash(self, account_name : str) -> str :
        account_import = self.__import_data_lookup[account_name]
        return get_imported_account_hash(self.__account_data_path, account_import)
    
    def is_stored(self, account_name : str) -> bool :
        return account_name in self.__import_data_lookup
    
    def get_account_names(self) -> typing.List[str] :
        return sorted(self.__import_data_lookup.keys())

    def get_account(self, account_name : str) -> Account :
        if account_name not in self.__import_data_lookup :
            logger.info(f"Account {account_name} not found in import data!")
            return Account()
        
        result_hash = self.get_account_hash(account_name)
        stored_hash = self.__hash_checker.get_stored_hash(account_name)
        if stored_hash == result_hash :
            #hash same, no action
            return self.__db.retrieve(account_name, Account)
        
        account = self.__import_account(account_name)
        if isinstance(account, Account) :
            self.__hash_checker.set_stored_hash(account_name, result_hash)
            self.__db.update(account_name, account)
            return account
        logger.warning(f"Failed to find account {account_name}, returning default")
        return Account(account_name)

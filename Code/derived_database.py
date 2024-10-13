import typing
from pathlib import Path

from Code.database import JsonDataBase
from Code.object_cacher import ObjectCacher
from Code.source_database import SourceDataBase
from Code.Data.account_data import Account, DerivedAccount
from Code.Pipeline.account_derivation import get_derived_account, get_derived_account_hash

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

class DerivedDataBase(JsonDataBase) :
    
    def __init__(self, hash_db : JsonDataBase, source_db : SourceDataBase, ledger_output_path : Path, account_derivations : typing.List[DerivedAccount]) :
        super().__init__(ledger_output_path, "DerivedAccounts")
        self.__cache = ObjectCacher(hash_db, "DerivedAccountHashes", Account())
        self.__derived_data_lookup = {}
        self.__source_db = source_db

        for account_derivation in account_derivations :
            self.__derived_data_lookup[account_derivation.name] = account_derivation
        for account_derivation in account_derivations :
            self.get_account(account_derivation.name)

    def __derive_account(self, account_name : str) -> Account | None :
        account_derivation = self.__derived_data_lookup[account_name]
        account = get_derived_account(self.__source_db, account_derivation)
        logger.info(f"Derived account {account_name}!")
        return account
    
    def get_account_hash(self, account_name : str) -> str :
        account_derivation = self.__derived_data_lookup[account_name]
        return get_derived_account_hash(self.__source_db, account_derivation)

    def get_account(self, account_name : str) -> Account :
        if account_name not in self.__derived_data_lookup :
            logger.info(f"Account {account_name} not found in derivation data!")
            return Account()

        current_hash = self.get_account_hash(account_name)
        return self.__cache.request_object(self, account_name, current_hash, self.__derive_account)

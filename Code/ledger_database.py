import typing
from pathlib import Path
from numpy import repeat
from polars import DataFrame, Series
from polars import concat

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

from Code.Pipeline.ledger_validation import get_ledger_entries_hash, get_ledger_entries
from Code.Pipeline.ledger_validation import get_unaccounted_transactions_hash, get_unaccounted_transactions

from Code.Data.account_data import Account
from Code.Data.account_data import LedgerConfiguration, AccountMapping, LedgerImport
from Code.Data.account_data import DataFrameObject

from Code.source_database import SourceDataBase
from Code.derived_database import DerivedDataBase
from Code.object_cacher import ObjectCacher
from Code.database import JsonDataBase
from Code.Utils.json_serializer import json_serializer

def make_account_data_table(account : Account) -> DataFrame :
    account_data = account.transactions[["date", "description", "delta"]]
    balance_list = []
    current_balance = account.start_value
    for transaction in account.transactions.rows() :
        current_balance += transaction[2]
        balance_list.append(round(current_balance, 2))
    balance_frame = DataFrame(Series("balance", balance_list))
    return concat([account_data, balance_frame], how="horizontal")

def get_ledger_configuration(dataroot_path : Path) -> LedgerConfiguration :
    ledger_config_path = dataroot_path / "LedgerConfiguration.json"
    try :
        read_object = json_serializer.read_from_file(ledger_config_path, LedgerConfiguration)
        assert isinstance(read_object, LedgerConfiguration), f"Failed to deserialize LedgerConfiguration! {ledger_config_path}"
        return read_object
    except Exception as e :
        logger.info(f"Failed to read ledger configuration from {ledger_config_path}! {e}")
    return LedgerConfiguration()

class LedgerDataBase :

    unaccounted_name = "UnaccountedTransactions"
    entries_name = "LedgerEntries"

    def __init__(self, root_path : Path, ledger_import : LedgerImport, account_mapping : AccountMapping) :
        ledger_output_path = root_path / ledger_import.ledger_name
        name = ledger_import.ledger_name
        self.__config_db = JsonDataBase(ledger_output_path, "Config")
        self.__cache = ObjectCacher(self.__config_db, "LedgerDataHashes", DataFrameObject())
        self.__account_mapping = account_mapping

        try :
            logger.info(f"Creating source database for {name}")
            account_data_path = root_path / ledger_import.source_account_folder
            source_db = SourceDataBase(self.__config_db, ledger_output_path, ledger_import.raw_accounts, account_data_path)
            logger.info(f"Source database created for {name}")
            self.__source_db = source_db
        except Exception as e :
            logger.error(f"Failed to build source database for ledger {name}! {e}")

        try :
            logger.info(f"Creating derived database for {name}")
            derived_db = DerivedDataBase(self.__config_db, self.__source_db, ledger_output_path, account_mapping.derived_accounts)
            logger.info(f"Derived database created for {name}")
            self.__derived_db = derived_db
        except Exception as e :
            logger.error(f"Failed to build derived database for ledger {name}! {e}")

        self.get_ledger_entries_table()
        self.get_unaccounted_transaction_table()

    def account_is_created(self, account_name : str) -> bool :
        return self.__source_db.is_stored(account_name) != self.__derived_db.is_stored(account_name)

    def get_account(self, account_name : str) -> Account :
        if self.__source_db.is_stored(account_name) :
            return self.__source_db.get_account(account_name)
        else :
            assert self.__derived_db.is_stored(account_name), f"Account {account_name} is not in base or derived DBs?"
            return self.__derived_db.get_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__source_db.get_names()
    
    def get_derived_account_names(self) -> typing.List[str] :
        return self.__derived_db.get_names()

    def get_ledger_data(self, name : str) -> DataFrameObject :
        match name :
            case LedgerDataBase.unaccounted_name :
                df = get_unaccounted_transactions(self.__account_mapping, self.__source_db)
            case LedgerDataBase.entries_name :
                df = get_ledger_entries(self.__account_mapping, self.__source_db)
            case _ :
                df = DataFrame()
        return DataFrameObject(df)

    def get_ledger_entries_table(self) -> DataFrame :
        try :
            ledger_entries_hash = get_ledger_entries_hash(self.__account_mapping, self.__source_db)
            return self.__cache.request_object(self.__config_db, LedgerDataBase.entries_name, ledger_entries_hash, self.get_ledger_data).frame
        except Exception as e :
            logger.error(f"Failed to verify ledger entries! {e}")
        return DataFrame()

    def get_unaccounted_transaction_table(self) -> DataFrame :
        try:
            unaccounted_transactions_hash = get_unaccounted_transactions_hash(self.__account_mapping, self.__source_db)
            return self.__cache.request_object(self.__config_db, LedgerDataBase.unaccounted_name, unaccounted_transactions_hash, self.get_ledger_data).frame
        except Exception as e :
            logger.error(f"Failed to calculate unaccounted transactions! {e}")
        return DataFrame()

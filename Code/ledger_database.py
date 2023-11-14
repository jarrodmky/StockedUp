import typing
from pathlib import Path
from numpy import repeat, absolute, negative, not_equal, any
from hashlib import sha256
from os import walk

from pandas import DataFrame, Series, Index, concat

from PyJMy.json_file import json_read, json_write
from PyJMy.debug import debug_assert, debug_message
from PyJMy.utf8_file import utf8_file

from csv_importing import read_transactions_from_csv_in_path
from database import JsonDataBase

from accounting_objects import Account, UniqueHashCollector, make_hasher, hash_float
from accounting_objects import AccountImport

def transaction_hash(index : int, date : str, timestamp : float, delta : float, description : str) -> int :
    hasher = make_hasher()
    
    hasher.update(date.encode())
    hash_float(hasher, timestamp)
    hash_float(hasher, delta)
    hasher.update(description.encode())
    new_id = int.from_bytes(hasher.digest(12), 'big')
    new_id <<= 32 #(4*8) pad 4 bytes
    new_id += index
    return new_id

derived_transaction_columns = ["date", "delta", "description", "timestamp", "source_ID", "source_account"]
unidentified_transaction_columns = ["date", "delta", "description", "timestamp"]
transaction_columns = ["ID", "date", "delta", "description", "timestamp"]
ledger_columns = ["from_account_name", "from_transaction_id", "to_account_name", "to_transaction_id", "delta"]

def make_identified_transaction_dataframe(transactions : DataFrame) -> DataFrame :
    make_id = lambda t : transaction_hash(int(t.name), t.date, t.timestamp, t.delta, t.description)
    transactions.insert(0, "ID", transactions.apply(make_id, axis=1, result_type="reduce"))
    return transactions

def file_hash(hasher : typing.Any, file_path : Path) -> None :
    buffer_size = (2 ** 20)
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            hasher.update(data)

def folder_hash(hasher : typing.Any, folder_path : Path) -> None :
    for dirpath, _, filenames in walk(folder_path, onerror=print) :
        for filename in filenames :
            current_file = Path(dirpath) / filename
            debug_message(f"[HASH] WITH {current_file}")
            file_hash(hasher, current_file)

def raw_account_data_hash(folder_path : Path, number : float) -> int :
    sha256_hasher = sha256()
    folder_hash(sha256_hasher, folder_path)
    hash_float(sha256_hasher, number)
    return int(sha256_hasher.hexdigest(), 16)

def try_import_raw_account(hash_register : typing.Optional[UniqueHashCollector], raw_account_path : Path, start_balance : float) -> Account | None :
    account_name = raw_account_path.stem
    try :
        read_transactions = read_transactions_from_csv_in_path(raw_account_path)
        if len(read_transactions) > 0 :
            read_transactions = make_identified_transaction_dataframe(read_transactions)
            assert read_transactions.columns.equals(Index(transaction_columns))
            account = Account(hash_register, account_name, start_balance, read_transactions)
            return account
    except Exception as e :
        debug_message(f"When importing from {str(raw_account_path)}, hit exception:\n[EXCEPT][{e}]")
    return None


class LedgerDataBase :

    ledger_entires_object_name = "LedgerEntries"
    source_account_input_hashes_object_name = "SourceAccountHashes"

    def __init__(self, hasher : typing.Any, root_path : Path, name : str, account_import_list : typing.List[AccountImport], on_import_raw_account : typing.Callable) :
        self.__data_path = root_path
        self.__ledgerfile_path = root_path / name
        self.__configuration_data = JsonDataBase(self.__ledgerfile_path, "Config")
        self.__base_account_data = JsonDataBase(self.__ledgerfile_path, "BaseAccounts")
        self.derived_account_data = JsonDataBase(self.__ledgerfile_path, "DerivedAccounts")

        self.__on_import = on_import_raw_account
        self.__hash_register = hasher
        account_data_pairs = [(Path(account_import.folder).stem, account_import) for account_import in account_import_list]
        for account_name, import_data in account_data_pairs :
            self.__check_source_account(account_name, import_data)
        self.__account_import_data = dict(account_data_pairs)

    def retrieve_ledger_entries(self) -> DataFrame :
        object_name = LedgerDataBase.ledger_entires_object_name
        if self.__configuration_data.is_stored(object_name) :
            return DataFrame.from_records(self.__configuration_data.retrieve(object_name)["entries"], columns=ledger_columns)
        else :
            new_ledger_entries = DataFrame(columns=ledger_columns)
            self.update_ledger_entries(new_ledger_entries)
            return new_ledger_entries

    def update_ledger_entries(self, ledger_entries : DataFrame) -> None :
        object_name = LedgerDataBase.ledger_entires_object_name
        self.__configuration_data.update(object_name, {"entries" : ledger_entries.to_dict("records")})
        
    def __get_source_hashes(self) :
        object_name = LedgerDataBase.source_account_input_hashes_object_name
        if self.__configuration_data.is_stored(object_name) :
            return self.__configuration_data.retrieve(object_name)
        else :
            self.__configuration_data.store(object_name, {})
            return {}
        
    def __update_source_hashes(self, source_hashes) :
        self.__configuration_data.update(LedgerDataBase.source_account_input_hashes_object_name, source_hashes)

    def __check_source_account(self, account_name : str, import_data : AccountImport) -> Account | None :
        raw_account_path = self.__data_path / import_data.folder
        start_balance = import_data.opening_balance
        source_hashes = self.__get_source_hashes()
        account = None
        if account_name in source_hashes :
            #previously imported, check hash
            old_hash = source_hashes[account_name]
            current_hash = raw_account_data_hash(raw_account_path, start_balance)
            if old_hash != current_hash :
                account = try_import_raw_account(self.__hash_register, raw_account_path, start_balance)
                if account is not None :
                    self.__base_account_data.update(account_name, account)
                    self.__on_import(account_name, account)
                    source_hashes[account_name] = current_hash
                else :
                    del source_hashes[account_name]
            else :
                account = self.__base_account_data.retrieve(account_name)
        else :
            #try read from new source
            account = try_import_raw_account(self.__hash_register, raw_account_path, start_balance)
            if account is not None :
                self.__base_account_data.update(account_name, account)
                self.__on_import(account_name, account)
                source_hashes[account_name] = raw_account_data_hash(raw_account_path, start_balance)

        self.__update_source_hashes(source_hashes)
        return account

    def get_source_account(self, account_name : str) -> Account | None :
        import_data = self.__account_import_data[account_name]
        return self.__check_source_account(account_name, import_data)
    
    def get_source_names(self) -> typing.List[str] :
        return self.__base_account_data.get_names()

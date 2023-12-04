import typing
from pathlib import Path
from hashlib import sha256, shake_256
from os import walk

from pandas import DataFrame, Index, Series
from PyJMy.debug import debug_message, debug_assert

from csv_importing import read_transactions_from_csv_in_path
from database import JsonDataBase, SQLDataBase
from accounting_objects import Account
from accounting_objects import AccountImport

class UniqueHashCollector :

    def __init__(self) :
        self.__hash_map : typing.Dict[str, typing.Dict[int, str]] = {}

    def register_hash(self, name_space : str, hash_code : int, hash_hint : str) -> None :
        if name_space not in self.__hash_map :
            self.__hash_map[name_space] = {}

        type_hash_map : typing.Dict[int, str] = self.__hash_map[name_space]
        debug_assert(hash_code not in type_hash_map, "Hash collision! " + str(hash_code) + " from (" + hash_hint + "), existing = (" + type_hash_map.get(hash_code, "ERROR!") + ")")
        type_hash_map[hash_code] = hash_hint

def hash_float(hasher : typing.Any, float_number : float) -> None :
    num, den = float_number.as_integer_ratio()
    hasher.update(num.to_bytes(8, 'big', signed=True))
    hasher.update(den.to_bytes(8, 'big'))

def transaction_hash(index : int, date : str, timestamp : float, delta : float, description : str) -> int :
    hasher = shake_256()
    
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

def managed_account_data_hash(hash_collector : UniqueHashCollector, account : Account) -> int :
    hasher = shake_256()
    hasher.update(account.name.encode())
    hash_float(hasher, account.start_value)
    for _, t in account.transactions.iterrows() :
        hasher.update(t.ID.to_bytes(16, 'big'))
        hash_collector.register_hash("Transaction", t.ID, f"Acct={account.name}, ID={t.ID}, Desc={t.description}")
    hash_float(hasher, account.end_value)
    return int.from_bytes(hasher.digest(16), 'big')

def raw_account_data_hash(folder_path : Path, number : float) -> int :
    sha256_hasher = sha256()
    folder_hash(sha256_hasher, folder_path)
    hash_float(sha256_hasher, number)
    return int(sha256_hasher.hexdigest(), 16)

def try_import_raw_account(hash_register : UniqueHashCollector, raw_account_path : Path, start_balance : float) -> Account | None :
    account_name = raw_account_path.stem
    try :
        read_transactions = read_transactions_from_csv_in_path(raw_account_path)
        if len(read_transactions) > 0 :
            read_transactions = make_identified_transaction_dataframe(read_transactions)
            assert read_transactions.columns.equals(Index(transaction_columns))
            account = Account(account_name, start_balance, read_transactions)
            account.ID = managed_account_data_hash(hash_register, account)
            hash_register.register_hash("Account", account.ID, f"Acct={account.name}")
            return account
    except Exception as e :
        debug_message(f"When importing from {str(raw_account_path)}, hit exception:\n[EXCEPT][{e}]")
    return None

def make_account_data_table(account : Account) -> DataFrame :
    account_data = account.transactions[["date", "description", "delta"]]
    balance_list = []
    current_balance = account.start_value
    for _, transaction in account.transactions.iterrows() :
        current_balance += transaction.delta
        balance_list.append(round(current_balance, 2))
    return account_data.join(Series(balance_list, name="Balance"))

class LedgerDataBase :

    ledger_entires_object_name = "LedgerEntries"
    source_account_input_hashes_object_name = "SourceAccountHashes"

    def __init__(self, hasher : typing.Any, root_path : Path, name : str, account_import_list : typing.List[AccountImport]) :
        self.__data_path = root_path
        self.__ledgerfile_path = root_path / name
        self.__configuration_data = JsonDataBase(self.__ledgerfile_path, "Config")
        self.__base_account_data = JsonDataBase(self.__ledgerfile_path, "BaseAccounts")
        self.__managed_account_data = SQLDataBase(self.__ledgerfile_path, "ManagedAccounts")
        self.derived_account_data = JsonDataBase(self.__ledgerfile_path, "DerivedAccounts")

        self.__hash_register = hasher
        account_data_pairs = [(Path(account_import.folder).stem, account_import) for account_import in account_import_list]
        for account_name, import_data in account_data_pairs :
            self.__check_source_account(account_name, import_data)

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
        
    def __get_stored_hashes(self) :
        object_name = LedgerDataBase.source_account_input_hashes_object_name
        if self.__configuration_data.is_stored(object_name) :
            return self.__configuration_data.retrieve(object_name)
        else :
            self.__configuration_data.store(object_name, {})
            return {}
    
    def __import_raw_account(self, account_name : str, raw_account_path : Path, start_balance : float) -> bool :
        account = try_import_raw_account(self.__hash_register, raw_account_path, start_balance)
        if account is not None :
            self.__base_account_data.update(account_name, account)
            self.__managed_account_data.update(account_name, make_account_data_table(account))
            return True
        return False
    
    def create_derived_account(self, account_name : str, account : Account) -> None :
        self.derived_account_data.store(account_name, account)
        self.__managed_account_data.store(account_name, make_account_data_table(account))
    
    def drop_derived_account(self, account_name : str) -> None :
        self.__managed_account_data.drop(account_name)
        self.derived_account_data.drop(account_name)

    def __check_source_account(self, account_name : str, import_data : AccountImport) -> None :
        raw_account_path = self.__data_path / import_data.folder
        start_balance = import_data.opening_balance
        calculated_hash = 0
        if raw_account_path.exists() or raw_account_path.is_dir() :
            calculated_hash = raw_account_data_hash(raw_account_path, start_balance)

        import_account = lambda : self.__import_raw_account(account_name, raw_account_path, start_balance)
        self.__work_if_needed(account_name, calculated_hash, import_account)

    def __work_if_needed(self, identifier : str, input_hash : int, work_cb : typing.Callable) -> None :
        stored_hash = self.__get_stored_hash(identifier)
        if stored_hash == input_hash :
            #hash same, no action
            return

        if stored_hash == 0 :
            #not previously imported, calculated_hash not zero (nontrivial data available)
            if work_cb() :
                self.__set_stored_hash(identifier, input_hash)
        else :
            #previously imported
            if input_hash == 0 :
                #trivial data or something removed
                debug_message("Data deleted! clearing hash.")
                self.__set_stored_hash(identifier, 0)
            else :
                #try update
                if work_cb() :
                    self.__set_stored_hash(identifier, input_hash)
                else :
                    #update failed
                    debug_message("Update failed! Keeping old data for safety.")

    
    def __get_stored_hash(self, name : str) -> int :
        source_hashes = self.__get_stored_hashes()
        if name in source_hashes :
            stored_hash = source_hashes[name]
            assert stored_hash != 0, "Stored 0 hashes forbidden, means import never done or invalid!"
            return stored_hash
        else :
            return 0

    def __set_stored_hash(self, name : str, new_hash : int) -> None :
        assert self.__get_stored_hash(name) != new_hash, "Setting new hash without checking it?"
        source_hashes = self.__get_stored_hashes()
        if new_hash != 0 :
            source_hashes[name] = new_hash
        else :
            if name in source_hashes :
                del source_hashes[name]
            else :
                debug_message("Zeroing out hash, something destructive or erroneous happened!")
        self.__configuration_data.update(LedgerDataBase.source_account_input_hashes_object_name, source_hashes)

    def account_is_created(self, account_name : str) -> bool :
        return self.__managed_account_data.is_stored(account_name)

    def get_account(self, account_name : str) -> Account :
        if self.__base_account_data.is_stored(account_name) :
            return self.__base_account_data.retrieve(account_name)
        else :
            assert self.derived_account_data.is_stored(account_name), f"Account {account_name} is not in base or derived DBs?"
            return self.derived_account_data.retrieve(account_name)

    def get_account_data_table(self, account_name : str) -> DataFrame :
        return self.__managed_account_data.retrieve(account_name)
    
    def get_source_names(self) -> typing.List[str] :
        return self.__base_account_data.get_names()

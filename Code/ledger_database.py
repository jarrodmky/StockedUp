import typing
from pathlib import Path
from hashlib import sha256, shake_256
from os import walk
from numpy import repeat, absolute
from pandas import DataFrame, Index, Series, concat

from PyJMy.debug import debug_message, debug_assert
from PyJMy.json_file import json_read, json_encoder
from PyJMy.utf8_file import utf8_file

from csv_importing import read_transactions_from_csv_in_path
from database import JsonDataBase, to_json_string
from accounting_objects import Account, AccountImport, DerivedAccount

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

def hash_object(hasher : typing.Any, some_object : typing.Any) -> None :
    hasher.update(to_json_string(some_object, cls=json_encoder).encode("utf-8"))

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

def folder_csvs_hash(hasher : typing.Any, folder_path : Path) -> None :
    for dirpath, _, filenames in walk(folder_path, onerror=print) :
        for filename in filenames :
            current_file = Path(dirpath) / filename
            if current_file.suffix == ".csv" :
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
    if not folder_path.exists() or not folder_path.is_dir() :
        return 0

    sha256_hasher = sha256()
    folder_csvs_hash(sha256_hasher, folder_path)
    hash_float(sha256_hasher, number)
    return int(sha256_hasher.hexdigest(), 16)

def get_account_imports(dataroot : Path, ledger_name : str) -> typing.List[AccountImport] :
    ledger_config = json_read(dataroot.joinpath("LedgerConfiguration.json"))
    for ledger_import in ledger_config.ledgers :
        if ledger_import.name == ledger_name :
            return ledger_import.raw_accounts
    return []

def get_account_derivations_internal(accountmapping_file : Path) -> typing.List[DerivedAccount] :
    if not accountmapping_file.exists() :
        with utf8_file(accountmapping_file, 'x') as new_mapping_file :
            new_mapping_file.write("{\n")
            new_mapping_file.write("\t\"derived accounts\": [],\n")
            new_mapping_file.write("\t\"internal transactions\": []\n")
            new_mapping_file.write("}")
        return []
    else :
        return json_read(accountmapping_file)["derived accounts"]

def get_account_derivations(dataroot : Path, ledger_name : str) -> typing.List[DerivedAccount] :
    ledger_config = json_read(dataroot.joinpath("LedgerConfiguration.json"))
    for ledger_import in ledger_config.ledgers :
        if ledger_import.name == ledger_name :
            account_mapping_file_path = dataroot / (ledger_import.accounting_file + ".json")
            return get_account_derivations_internal(account_mapping_file_path)
    return []

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

def escape_string(string : str) -> str :
    return string.replace("*", "\*").replace("+", "\+").replace("(", "\(").replace(")", "\)")

def strings_to_regex(strings : typing.List[str]) -> str :
    return "|".join([escape_string(s) for s in strings])

def get_matched_transactions(match_account : Account, string_matches : typing.List[str]) -> DataFrame :
    account_name = match_account.name
    debug_assert(match_account is not None, f"Account not found! Expected account \"{account_name}\" to exist!")
    debug_message(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
    
    matched_indices = match_account.transactions["description"].str.contains(strings_to_regex(string_matches))
    match_tuples = match_account.transactions[matched_indices]

    debug_message(f"Found {len(match_tuples)} transactions in {account_name}")
    return match_tuples

def derive_transaction_dataframe(account_name : str, dataframe : DataFrame) -> DataFrame :
    return DataFrame({
        "date" : dataframe.date,
        "delta" : -dataframe.delta,
        "description" : dataframe.description,
        "timestamp" : dataframe.timestamp,
        "source_ID" : dataframe.ID,
        "source_account" : repeat(account_name, len(dataframe))
    })

class CheckedHashWorker :

    def __init__(self, hash_db : JsonDataBase, name : str) :
        self.__hash_db = hash_db
        self.__hash_object_name = name + "Hashes"

    def __get_stored_hash(self, name : str) -> int :
        source_hashes = self.__get_stored_hashes()
        if name in source_hashes :
            stored_hash = source_hashes[name]
            assert stored_hash != 0, "Stored 0 hashes forbidden, means import never done or invalid!"
            return stored_hash
        else :
            return 0
        
    def __get_stored_hashes(self) :
        if self.__hash_db.is_stored(self.__hash_object_name) :
            return self.__hash_db.retrieve(self.__hash_object_name)
        else :
            self.__hash_db.store(self.__hash_object_name, {})
            return {}

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
        self.__hash_db.update(self.__hash_object_name, source_hashes)

    def work_if_needed(self, identifier : str, input_hash : int, work_cb : typing.Callable) -> None :
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

class SourceAccountDatabase :

    def __init__(self, dataroot_path : Path, ledgerfile_path : Path, config_db : JsonDataBase, hasher : typing.Any) :
        self.__data_path = dataroot_path
        self.__check_worker = CheckedHashWorker(config_db, "SourceAccount")
        self.__account_data = JsonDataBase(ledgerfile_path, "BaseAccounts")
        self.__hash_register = hasher
        self.__account_imports = dict((Path(account_import.folder).stem, account_import) for account_import in get_account_imports(dataroot_path, ledgerfile_path.stem))

        for account_name in self.__account_imports.keys() :
            self.__check_source_account(account_name)

    def get_source_account(self, account_name : str) -> Account :
        self.__check_source_account(account_name)
        assert self.__account_data.is_stored(account_name)
        return self.__account_data.retrieve(account_name)

    def get_source_account_names(self) -> typing.List[str] :
        return self.__account_data.get_names()

    def has_source_account(self, account_name : str) -> bool :
        return self.__account_data.is_stored(account_name)
    
    def __import_raw_account(self, account_name : str, raw_account_path : Path, start_balance : float) -> bool :
        account = try_import_raw_account(self.__hash_register, raw_account_path, start_balance)
        if account is not None :
            self.__account_data.update(account_name, account)
            return True
        return False

    def __check_source_account(self, account_name : str) -> None :
        assert account_name in self.__account_imports, f"Unknown source account {account_name}, maybe update import list?"

        import_data = self.__account_imports[account_name]
        raw_account_path = self.__data_path / import_data.folder
        start_balance = import_data.opening_balance
        calculated_hash = raw_account_data_hash(raw_account_path, start_balance)

        import_account = lambda : self.__import_raw_account(account_name, raw_account_path, start_balance)
        self.__check_worker.work_if_needed(account_name, calculated_hash, import_account)

def get_derived_matched_transactions(source_database : SourceAccountDatabase, derived_account_mapping : DerivedAccount) -> DataFrame :
    matched_transaction_frames = []

    if len(derived_account_mapping.matchings) == 1 and derived_account_mapping.matchings[0] is not None and derived_account_mapping.matchings[0].account_name == "" :
        universal_match_strings = derived_account_mapping.matchings[0].strings
        debug_message(f"Checking all base accounts for {universal_match_strings}")
        for account_name in source_database.get_source_account_names() :
            found_tuples = get_matched_transactions(source_database.get_source_account(account_name), universal_match_strings)
            matched_transaction_frames.append(derive_transaction_dataframe(account_name, found_tuples))
            
    else :
        for matching in derived_account_mapping.matchings :
            if matching.account_name == "" :
                raise RuntimeError(f"Nonspecific match strings detected for account {derived_account_mapping.name}! Not compatible with specified accounts!")
            debug_message(f"Checking {matching.account_name} account for {matching.strings}")
            found_tuples = get_matched_transactions(source_database.get_source_account(matching.account_name), matching.strings)
            matched_transaction_frames.append(derive_transaction_dataframe(matching.account_name, found_tuples))
    
    all_matched_transactions = concat(matched_transaction_frames, ignore_index=True)
    all_matched_transactions.sort_values(by=["timestamp"], kind="stable", ignore_index=True, inplace=True)
    return all_matched_transactions

def derived_account_data_hash(source_database : SourceAccountDatabase, derived_account_mapping : DerivedAccount) -> int :
    sha256_hasher = sha256()
    for account_name in sorted(source_database.get_source_account_names()) :
        sha256_hasher.update(source_database.get_source_account(account_name).ID.to_bytes(16))
    hash_object(sha256_hasher, derived_account_mapping)
    return int(sha256_hasher.hexdigest(), 16)

class LedgerEntryFrame :

    ledger_entires_object_name = "LedgerEntries"

    def __init__(self, config_db : JsonDataBase) :
        self.__configuration_data = config_db

    def retrieve(self) -> DataFrame :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        if self.__configuration_data.is_stored(object_name) :
            return DataFrame.from_records(self.__configuration_data.retrieve(object_name)["entries"], columns=ledger_columns)
        else :
            new_ledger_entries = DataFrame(columns=ledger_columns)
            self.update(new_ledger_entries)
            return new_ledger_entries

    def update(self, ledger_entries : DataFrame) -> None :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        self.__configuration_data.update(object_name, {"entries" : ledger_entries.to_dict("records")})

class DerivedAccountDatabase :

    def __init__(self, dataroot_path : Path, ledgerfile_path : Path, source_db : SourceAccountDatabase, config_db : JsonDataBase, hasher : typing.Any, account_entries : typing.Callable) :
        self.__source_db = source_db
        self.__check_worker = CheckedHashWorker(config_db, "DerivedAccount")
        self.__account_data = JsonDataBase(ledgerfile_path, "DerivedAccounts")
        self.__hash_register = hasher
        self.__account_derivations = dict((account_import.name, account_import) for account_import in get_account_derivations(dataroot_path, ledgerfile_path.stem))
        self.__account_entries = account_entries

        for account_name in self.__account_derivations.keys() :
            self.__check_derived_account(account_name)

    def get_derived_account(self, account_name : str) -> Account :
        self.__check_derived_account(account_name)
        assert self.__account_data.is_stored(account_name)
        return self.__account_data.retrieve(account_name)

    def get_derived_account_names(self) -> typing.List[str] :
        return self.__account_data.get_names()

    def has_derived_account(self, account_name : str) -> bool :
        return self.__account_data.is_stored(account_name)

    def __derive_account(self, account_name : str, account_mapping : DerivedAccount) -> bool :
        debug_message(f"Mapping spending account \"{account_name}\"")
        derived_transactions = get_derived_matched_transactions(self.__source_db, account_mapping)
        if len(derived_transactions) > 0 :
            assert account_name not in derived_transactions.source_account.unique(), "Transaction to same account forbidden!"

            derived_transactions = make_identified_transaction_dataframe(derived_transactions)

            try :
                derived_ledger_entries = DataFrame({
                    "from_account_name" : derived_transactions.source_account,
                    "from_transaction_id" : derived_transactions.source_ID,
                    "to_account_name" : repeat(account_name, len(derived_transactions.index)),
                    "to_transaction_id" : derived_transactions.ID,
                    "delta" : absolute(derived_transactions.delta)
                })
                self.__account_entries(derived_ledger_entries)
                derived_transactions = derived_transactions[transaction_columns]

                account = Account(account_name, account_mapping.start_value, derived_transactions)
                account.ID = managed_account_data_hash(self.__hash_register, account)
                self.__hash_register.register_hash("Account", account.ID, f"Acct={account.name}")

                self.__account_data.update(account_name, account)
                
                debug_message(f"... account {account_name} derived!")
                return True
            except Exception as e :
                if self.__account_data.is_stored(account_name) :
                    self.__account_data.drop(account_name)

                debug_message(f"... exception {e} when {account_name} was derived!")
        else :
            debug_message(f"... nothing to map for {account_name}!")
        return False

    def __check_derived_account(self, account_name : str) -> None :
        assert account_name in self.__account_derivations, f"Unknown derived account {account_name}, maybe update mapping list?"

        derivation_data = self.__account_derivations[account_name]
        calculated_hash = derived_account_data_hash(self.__source_db, derivation_data)

        derive_account = lambda : self.__derive_account(account_name, derivation_data)
        self.__check_worker.work_if_needed(account_name, calculated_hash, derive_account)


class LedgerDataBase :

    def __init__(self, hasher : typing.Any, root_path : Path, name : str, account_entries : typing.Callable) :
        ledgerfolder_path = root_path / name
        self.__configuration_data = JsonDataBase(ledgerfolder_path, "Config")
        self.ledger_entries = LedgerEntryFrame(self.__configuration_data)
        self.__source_account_data = SourceAccountDatabase(root_path, ledgerfolder_path, self.__configuration_data, hasher)
        account_entries_to_ledger = lambda df : account_entries(df, self.ledger_entries)
        self.__derived_account_data = DerivedAccountDatabase(root_path, ledgerfolder_path, self.__source_account_data, self.__configuration_data, hasher, account_entries_to_ledger)

    def account_is_created(self, account_name : str) -> bool :
        return self.__source_account_data.has_source_account(account_name) != self.__derived_account_data.has_derived_account(account_name)

    def get_account(self, account_name : str) -> Account :
        if self.__source_account_data.has_source_account(account_name) :
            return self.__source_account_data.get_source_account(account_name)
        else :
            assert self.__derived_account_data.has_derived_account(account_name), f"Account {account_name} is not in base or derived DBs?"
            return self.__derived_account_data.get_derived_account(account_name)

    def get_account_data_table(self, account_name : str) -> DataFrame :
        return make_account_data_table(self.get_account(account_name))
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__source_account_data.get_source_account_names()
    
    def get_derived_account_names(self) -> typing.List[str] :
        return self.__derived_account_data.get_derived_account_names()
    
    def get_source_accounts(self) -> typing.Generator[Account, None, None] :
        for account_name in self.__source_account_data.get_source_account_names() :
            yield self.__source_account_data.get_source_account(account_name)

import typing
from pathlib import Path
from hashlib import sha256
from numpy import repeat
from polars import DataFrame, Series, String, Float64
from polars import concat, from_dicts, col

from Code.PyJMy.json_file import json_read, json_encoder
from Code.PyJMy.utf8_file import utf8_file

from Code.logger import get_logger
logger = get_logger(__name__)

from Code.Pipeline.account_importing import import_ledger_source_accounts

from Code.Data.account_data import transaction_columns, ledger_columns, Account
from Code.Data.hashing import managed_account_data_hash, make_identified_transaction_dataframe

from Code.database import JsonDataBase, to_json_string
from Code.accounting_objects import DerivedAccount

def hash_object(hasher : typing.Any, some_object : typing.Any) -> None :
    hasher.update(to_json_string(some_object, cls=json_encoder).encode("utf-8"))

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

def make_account_data_table(account : Account) -> DataFrame :
    account_data = account.transactions[["date", "description", "delta"]]
    balance_list = []
    current_balance = account.start_value
    for transaction in account.transactions.rows() :
        current_balance += transaction[2]
        balance_list.append(round(current_balance, 2))
    balance_frame = DataFrame(Series("Balance", balance_list))
    return concat([account_data, balance_frame], how="horizontal")

def escape_string(string : str) -> str :
    return string.replace("*", "\*").replace("+", "\+").replace("(", "\(").replace(")", "\)")

def strings_to_regex(strings : typing.List[str]) -> str :
    return "|".join([escape_string(s) for s in strings])

def get_matched_transactions(match_account : Account, string_matches : typing.List[str]) -> DataFrame :
    account_name = match_account.name
    assert match_account is not None, f"Account not found! Expected account \"{account_name}\" to exist!"
    logger.info(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
    
    regex = strings_to_regex(string_matches)
    matched_transactions = match_account.transactions.filter(col("description").str.contains(regex))

    logger.info(f"Found {matched_transactions.height} transactions in {account_name}")
    return matched_transactions

def derive_transaction_dataframe(account_name : str, dataframe : DataFrame) -> DataFrame :
    return DataFrame({
        "date" : dataframe["date"],
        "delta" : -dataframe["delta"],
        "description" : dataframe["description"],
        "timestamp" : dataframe["timestamp"],
        "source_ID" : dataframe["ID"],
        "source_account" : repeat(account_name, dataframe.height)
    })

def import_and_register_source_accounts(dataroot_path : Path, ledger_name : str, hasher : typing.Any) -> typing.List[Account] :
    ledger_config_path = dataroot_path / "LedgerConfiguration.json"
    imported_accounts = []
    try :
        imported_accounts = import_ledger_source_accounts(ledger_config_path, dataroot_path, ledger_name)
    except Exception as e :
        logger.info(f"Failed to import accounts from {ledger_name}! {e}")

    for account in imported_accounts :
        account.ID = managed_account_data_hash(hasher, account)
        hasher.register_hash("Account", account.ID, f"Acct={account.name}")
    return imported_accounts

class SourceAccountDatabase :

    def __init__(self, ledgerfile_path : Path, source_accounts : typing.List[Account]) :
        self.__account_data = JsonDataBase(ledgerfile_path, "BaseAccounts")
        for account in source_accounts :
            self.__account_data.update(account.name, account)

    def get_source_account(self, account_name : str) -> Account :
        assert self.__account_data.is_stored(account_name)
        return self.__account_data.retrieve(account_name)

    def get_source_account_names(self) -> typing.List[str] :
        return self.__account_data.get_names()

    def has_source_account(self, account_name : str) -> bool :
        return self.__account_data.is_stored(account_name)

def get_derived_matched_transactions(source_database : SourceAccountDatabase, derived_account_mapping : DerivedAccount) -> DataFrame :
    matched_transaction_frames = []

    if len(derived_account_mapping.matchings) == 1 and derived_account_mapping.matchings[0] is not None and derived_account_mapping.matchings[0].account_name == "" :
        universal_match_strings = derived_account_mapping.matchings[0].strings
        logger.info(f"Checking all base accounts for {universal_match_strings}")
        for account_name in source_database.get_source_account_names() :
            found_tuples = get_matched_transactions(source_database.get_source_account(account_name), universal_match_strings)
            matched_transaction_frames.append(derive_transaction_dataframe(account_name, found_tuples))
            
    else :
        for matching in derived_account_mapping.matchings :
            if matching.account_name == "" :
                raise RuntimeError(f"Nonspecific match strings detected for account {derived_account_mapping.name}! Not compatible with specified accounts!")
            logger.info(f"Checking {matching.account_name} account for {matching.strings}")
            found_tuples = get_matched_transactions(source_database.get_source_account(matching.account_name), matching.strings)
            matched_transaction_frames.append(derive_transaction_dataframe(matching.account_name, found_tuples))
    
    all_matched_transactions = concat(matched_transaction_frames)
    return all_matched_transactions.sort(by="timestamp", maintain_order=True)

def derived_account_data_hash(source_database : SourceAccountDatabase, derived_account_mapping : DerivedAccount) -> int :
    sha256_hasher = sha256()
    for account_name in sorted(source_database.get_source_account_names()) :
        ID_int = int(source_database.get_source_account(account_name).ID)
        sha256_hasher.update(ID_int.to_bytes(16))
    hash_object(sha256_hasher, derived_account_mapping)
    return int(sha256_hasher.hexdigest(), 16)

class LedgerEntryFrame :

    ledger_entires_object_name = "LedgerEntries"

    def __init__(self, ledgerfolder_path : Path) :
        self.__configuration_data = JsonDataBase(ledgerfolder_path, "Config")

    def retrieve(self) -> DataFrame :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        if self.__configuration_data.is_stored(object_name) :
            return from_dicts(self.__configuration_data.retrieve(object_name)["entries"], schema=ledger_columns)
        else :
            empty_frame = DataFrame(schema={
                    "from_account_name" : String,
                    "from_transaction_id" : String,
                    "to_account_name" : String,
                    "to_transaction_id" : String,
                    "delta" : Float64
                    })
            self.update(empty_frame)
            return empty_frame

    def update(self, ledger_entries : DataFrame) -> None :
        object_name = LedgerEntryFrame.ledger_entires_object_name
        self.__configuration_data.update(object_name, {"entries" : ledger_entries.to_dicts()})

def create_and_register_derived_accounts(dataroot_path : Path, ledgerfile_path : Path, source_db : SourceAccountDatabase, hasher : typing.Any, account_entries : typing.Callable, ledger_entries : LedgerEntryFrame) -> typing.List[Account] :

    derived_accounts = []
    for account_import in get_account_derivations(dataroot_path, ledgerfile_path.stem) :
        account_name = account_import.name

        logger.info(f"Mapping spending account \"{account_name}\"")
        derived_transactions = get_derived_matched_transactions(source_db, account_import)
        if len(derived_transactions) > 0 :
            assert account_name not in derived_transactions["source_account"].unique(), "Transaction to same account forbidden!"

            derived_transactions = make_identified_transaction_dataframe(derived_transactions)

            try :
                derived_ledger_entries = DataFrame({
                    "from_account_name" : derived_transactions["source_account"],
                    "from_transaction_id" : derived_transactions["source_ID"],
                    "to_account_name" : Series(values=repeat(account_name, derived_transactions.height), dtype=String),
                    "to_transaction_id" : derived_transactions["ID"],
                    "delta" : derived_transactions["delta"].abs()
                })
                account_entries(derived_ledger_entries, ledger_entries)
                derived_transactions = derived_transactions[transaction_columns]

                account = Account(account_name, account_import.start_value, derived_transactions)
                derived_accounts.append(account)
                logger.info(f"... account {account_name} derived!")
            except Exception as e :
                logger.info(f"... hit exception ({e}) when trying to derive {account_name}!")
        else :
            logger.info(f"... nothing to map for {account_name}!")

    for derived_account in derived_accounts :
        derived_account.ID = managed_account_data_hash(hasher, derived_account)
        hasher.register_hash("Account", derived_account.ID, f"Acct={derived_account.name}")
    return derived_accounts

class DerivedAccountDatabase :

    def __init__(self, ledgerfile_path : Path, derived_accounts : typing.List[Account]) :
        self.__account_data = JsonDataBase(ledgerfile_path, "DerivedAccounts")
        for derived_account in derived_accounts :
            self.__account_data.update(derived_account.name, derived_account)

    def get_derived_account(self, account_name : str) -> Account :
        assert self.__account_data.is_stored(account_name)
        return self.__account_data.retrieve(account_name)

    def get_derived_account_names(self) -> typing.List[str] :
        return self.__account_data.get_names()

    def has_derived_account(self, account_name : str) -> bool :
        return self.__account_data.is_stored(account_name)


class LedgerDataBase :

    def __init__(self, hasher : typing.Any, root_path : Path, name : str, account_entries : typing.Callable) :
        ledgerfolder_path = root_path / name

        self.ledger_entries = LedgerEntryFrame(ledgerfolder_path)

        source_accounts = import_and_register_source_accounts(root_path, name, hasher)
        self.__source_account_data = SourceAccountDatabase(ledgerfolder_path, source_accounts)

        derived_accounts = create_and_register_derived_accounts(root_path, ledgerfolder_path, self.__source_account_data, hasher, account_entries, self.ledger_entries)
        self.__derived_account_data = DerivedAccountDatabase(ledgerfolder_path, derived_accounts)

    def account_is_created(self, account_name : str) -> bool :
        return self.__source_account_data.has_source_account(account_name) != self.__derived_account_data.has_derived_account(account_name)

    def get_account(self, account_name : str) -> Account :
        if self.__source_account_data.has_source_account(account_name) :
            return self.__source_account_data.get_source_account(account_name)
        else :
            assert self.__derived_account_data.has_derived_account(account_name), f"Account {account_name} is not in base or derived DBs?"
            return self.__derived_account_data.get_derived_account(account_name)
    
    def get_source_account_names(self) -> typing.List[str] :
        return self.__source_account_data.get_source_account_names()
    
    def get_derived_account_names(self) -> typing.List[str] :
        return self.__derived_account_data.get_derived_account_names()
    
    def get_source_accounts(self) -> typing.Generator[Account, None, None] :
        for account_name in self.__source_account_data.get_source_account_names() :
            yield self.__source_account_data.get_source_account(account_name)

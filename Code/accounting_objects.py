import typing
import hashlib
from pandas import DataFrame, Series

from PyJMy.debug import debug_assert
from PyJMy.json_file import json_register_writeable, json_register_readable

StringHashMap = typing.Dict[int, str]
ObjectDictionary = typing.Dict[str, typing.Any]

def make_hasher() :
    return hashlib.shake_256()

class UniqueHashCollector :

    def __init__(self) :
        self.hash_map : typing.Dict[type, StringHashMap] = {}

    def register_hash(self, hash_code : int, data_type : type, hash_hint : str) -> None :
        if data_type not in self.hash_map :
            self.hash_map[data_type] = {}

        type_hash_map : StringHashMap = self.hash_map[data_type]
        debug_assert(hash_code not in type_hash_map, "Hash collision! " + str(hash_code) + " from (" + hash_hint + "), existing = (" + type_hash_map.get(hash_code, "ERROR!") + ")")
        type_hash_map[hash_code] = hash_hint

def hash_float(hasher : typing.Any, float_number : float) -> None :
    num, den = float_number.as_integer_ratio()
    hasher.update(num.to_bytes(8, 'big', signed=True))
    hasher.update(den.to_bytes(8, 'big'))


class Account :
    
    class Transaction :
        pass

    def __init__(self, hash_register : typing.Optional[UniqueHashCollector] = None, name : str = "DEFAULT_ACCOUNT", start_value : float = 0.0, transactions : DataFrame = None) :
        self.name : str = name
        self.start_value : float = start_value
        self.transactions : DataFrame = transactions
        self.end_value : float = self.start_value
        if transactions is not None :
            self.end_value = round(self.start_value + sum(self.transactions["delta"]), 2)
            self.end_value = 0.0 if self.end_value == 0.0 else self.end_value #TODO negative zero outputs of sum?
        self.ID : int = 0

        if hash_register is not None :
            self.update_hash(hash_register)

    @staticmethod
    def decode(reader) :
        new_accout = Account()
        new_accout.ID =  reader["ID"]
        new_accout.name = reader["name"]
        new_accout.start_value = reader["start_value"]
        new_accout.end_value = reader["end_value"]
        new_accout.transactions = DataFrame.from_records(reader["transactions"])
        return new_accout
    
    def encode(self) :
        writer : ObjectDictionary = {}
        writer["ID"] = self.ID
        writer["name"] = self.name
        writer["start_value"] = self.start_value
        writer["end_value"] = self.end_value
        writer["transactions"] = self.transactions.to_dict("records")
        return writer

    def update_hash(self, hash_collector : UniqueHashCollector) -> None :
        hasher = make_hasher()
        hasher.update(self.name.encode())
        hash_float(hasher, self.start_value)

        for _, t in self.transactions.iterrows() :
            hasher.update(t.ID.to_bytes(16, 'big'))
            hash_collector.register_hash(t.ID, Account.Transaction, f"Acct={self.name}, ID={t.ID}, Desc={t.description}")

        hash_float(hasher, self.end_value)
        self.ID = int.from_bytes(hasher.digest(16), 'big')
        hash_collector.register_hash(self.ID, Account, f"Acct={self.name}")

    def make_account_data_table(self) -> DataFrame :
        account_data = self.transactions[["date", "description", "delta"]]
        balance_list = []
        current_balance = self.start_value
        for _, transaction in self.transactions.iterrows() :
            current_balance += transaction.delta
            balance_list.append(round(current_balance, 2))
        return account_data.join(Series(balance_list, name="Balance"))

json_register_writeable(Account)
json_register_readable(Account)

class DerivedAccount :

    class Matching :

        def __init__(self) :
            self.account_name : str = "<INVALID ACCOUNT>"
            self.strings : typing.List[str] = []

        @staticmethod
        def decode(reader) :
            new_matching = DerivedAccount.Matching()
            new_matching.account_name = reader["account name"]
            new_matching.strings = reader["strings"]
            return new_matching

    def __init__(self) :
        self.name = "<INVALID ACCOUNT>"
        self.matchings : typing.List[DerivedAccount.Matching] = []
        self.start_value = 0.0

    @staticmethod
    def decode(reader) :
        new_derived_account = DerivedAccount()
        new_derived_account.name = reader["name"]
        new_derived_account.matchings = reader["matchings"]
        new_derived_account.start_value = reader.read_optional("starting value", 0.0)
        return new_derived_account

json_register_readable(DerivedAccount)
json_register_readable(DerivedAccount.Matching)

class InternalTransactionMapping :

    def __init__(self) :
        self.from_account = "<INVALID ACCOUNT>"
        self.from_match_strings : typing.List[str] = []
        self.to_account = "<INVALID ACCOUNT>"
        self.to_match_strings : typing.List[str] = []

    @staticmethod
    def decode(reader) :
        new_transaction_mapping = InternalTransactionMapping()
        new_transaction_mapping.from_account = reader["from account"]
        new_transaction_mapping.from_match_strings = reader["from matchings"]
        new_transaction_mapping.to_account = reader["to account"]
        new_transaction_mapping.to_match_strings = reader["to matchings"]
        return new_transaction_mapping

json_register_readable(InternalTransactionMapping)

class AccountImport :

    def __init__(self) :
        self.folder : str = "<INVALID FOLDER>"
        self.opening_balance : float = 0.0

    @staticmethod
    def decode(reader) :
        new_account_import = AccountImport()
        new_account_import.folder = reader["folder"]
        new_account_import.opening_balance = reader.read_optional("opening balance", 0.0)
        return new_account_import

json_register_readable(AccountImport)

class LedgerImport :

    def __init__(self) :
        self.name : str = "<INVALID LEDGER>"
        self.accounting_file : str = "<INVALID FILE>"
        self.raw_accounts : typing.List[AccountImport] = []

    @staticmethod
    def decode(reader) :
        new_ledger_import = LedgerImport()
        new_ledger_import.name = reader["name"]
        new_ledger_import.accounting_file = reader["accounting file"]
        new_ledger_import.raw_accounts = reader["raw accounts"]
        return new_ledger_import

json_register_readable(LedgerImport)

class LedgerConfiguration :

    def __init__(self) :
        self.default_ledger : str = "<INVALID LEDGER>"
        self.ledgers : typing.List[LedgerImport] = []

    @staticmethod
    def decode(reader) :
        new_ledger_config = LedgerConfiguration()
        new_ledger_config.default_ledger = reader["default ledger"]
        new_ledger_config.ledgers = reader["ledgers"]
        return new_ledger_config

json_register_readable(LedgerConfiguration)

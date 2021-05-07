import pathlib
import typing
import hashlib

from json_file import json_register_writeable, json_register_readable, json_read, json_write
from debug import debug_assert

data_path = pathlib.Path("Data")
transaction_base_data_path = data_path.joinpath("TransactionBase")
transaction_derived_data_path = data_path.joinpath("TransactionDerived")
if not transaction_derived_data_path.exists() :
    transaction_derived_data_path.mkdir()
ledger_data_path = data_path.joinpath("Ledger")

ledger_data_file = ledger_data_path.joinpath("Entries.json")
account_data_file = ledger_data_path.joinpath("Accounts.json")

unique_hash_set = set()

class Transaction :

    def __init__(self, date : str = "", timestamp : float = 0.0, delta : float = 0.0, description : str = "") :
        self.date : str = date
        self.timestamp : float = timestamp
        self.delta : float = delta
        self.description : str = description
        self.ID : int = 0

    @staticmethod
    def decode(reader) :
        new_transaction = Transaction()
        new_transaction.ID =  reader["ID"]
        new_transaction.date = reader["date"]
        new_transaction.timestamp = reader["timestamp"]
        new_transaction.delta = reader["delta"]
        new_transaction.description = reader["description"]
        return new_transaction
    
    def encode(self) :
        writer = {}
        writer["ID"] = self.ID
        writer["date"] = self.date
        writer["timestamp"] = self.timestamp
        writer["delta"] = self.delta
        writer["description"] = self.description
        return writer

    def hash_internal(self) :
        hasher = hashlib.shake_256()
        hasher.update(self.date.encode())
        tsNum, tsDen = self.timestamp.as_integer_ratio()
        hasher.update(tsNum.to_bytes(8, 'big', signed=True))
        hasher.update(tsDen.to_bytes(8, 'big'))
        dtNum, dtDen = self.delta.as_integer_ratio()
        hasher.update(dtNum.to_bytes(8, 'big', signed=True))
        hasher.update(dtDen.to_bytes(8, 'big'))
        hasher.update(self.description.encode())
        self.ID = int.from_bytes(hasher.digest(16), 'big')
        debug_assert(self.ID not in unique_hash_set, "Hash collision ="+str(self.ID))
        unique_hash_set.add(self.ID)

json_register_writeable(Transaction)
json_register_readable(Transaction)

class Account :

    def __init__(self, name : str = "", start_value : float = 0.0, transactions : typing.List[Transaction] = []) :
        self.name : str = name
        self.start_value : float = start_value
        self.transactions : typing.List[Transaction] = transactions

        value = start_value
        for transaction in self.transactions :
            value = value + transaction.delta

        self.end_value : float = round(value, 2)
        self.ID : int = 0

    @staticmethod
    def decode(reader) :
        new_accout = Account()
        new_accout.ID =  reader["ID"]
        new_accout.name = reader["name"]
        new_accout.start_value = reader["start_value"]
        new_accout.end_value = reader["end_value"]
        new_accout.transactions = reader["transactions"]
        return new_accout
    
    def encode(self) :
        writer = {}
        writer["ID"] = self.ID
        writer["name"] = self.name
        writer["start_value"] = self.start_value
        writer["end_value"] = self.end_value
        writer["transactions"] = self.transactions
        return writer

    def hash_internal(self) :
        hasher = hashlib.shake_256()
        hasher.update(self.name.encode())
        svNum, svDen = self.start_value.as_integer_ratio()
        hasher.update(svNum.to_bytes(8, 'big', signed=True))
        hasher.update(svDen.to_bytes(8, 'big'))
        for transaction in self.transactions :
            hasher.update(transaction.ID.to_bytes(16, 'big'))
        evNum, evDen = self.end_value.as_integer_ratio()
        hasher.update(evNum.to_bytes(8, 'big', signed=True))
        hasher.update(evDen.to_bytes(8, 'big'))
        self.ID = int.from_bytes(hasher.digest(16), 'big')
        debug_assert(self.ID not in unique_hash_set, "Hash collision ="+str(self.ID))
        unique_hash_set.add(self.ID)


json_register_writeable(Account)
json_register_readable(Account)

class LedgerTransaction :

    def __init__(self, account_name : str, transaction : Transaction) :
        self.account_name : str = account_name
        self.ID : int = transaction.ID
        self.value : float = transaction.delta

class Ledger :

    LedgerEntryType = typing.Tuple[LedgerTransaction, LedgerTransaction]

    def __init__(self) :
        self.ledger_entries : typing.List[LedgerEntryType] = []
        self.transaction_lookup : typing.Set[int] = set()

    def add_to_ledger(self, from_account_name : str, from_transaction : Transaction, to_account_name : str, to_transaction : Transaction) :
        debug_assert(from_account_name != to_account_name, "Transaction to same account forbidden!")
        debug_assert(from_transaction.delta == -to_transaction.delta, "Transaction is not balanced credit and debit!")

        #insert from - to
        new_ledger_entry : LedgerEntryType = (LedgerTransaction(from_account_name, from_transaction), LedgerTransaction(to_account_name, to_transaction))
        self.ledger_entries.add(new_ledger_entry)
        self.transaction_lookup.add(from_transaction.ID)
        self.transaction_lookup.add(to_transaction.ID)

    def transaction_accounted(self, transaction_ID : int) -> bool :
        return (transaction_ID in self.transaction_lookup)



def load_accounts(directory : pathlib.Path) -> typing.List[Account] :

    base_accounts = []
    for base_account_file in directory.iterdir() :
        base_accounts.append(json_read(base_account_file))

    return base_accounts

def load_base_accounts() -> typing.List[Account] :
    return load_accounts(transaction_base_data_path)

def load_derived_accounts() -> typing.List[Account] :
    return load_accounts(transaction_derived_data_path)

def save_derived_account(account : Account) -> bool :
    write_file_path : pathlib.Path = transaction_derived_data_path.joinpath(account.name)
    json_write(write_file_path, account)


#json_register_writeable(Ledger)
#json_register_readable(Ledger)
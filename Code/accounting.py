import typing
import hashlib

from debug import debug_assert

from json_file import json_register_readable
from json_file import json_register_writeable

unique_hash_set = set()

class Transaction :

    def __init__(self, date : str = "", timestamp : float = 0.0, delta : float = 0.0, description : str = "") :
        self.date = date
        self.timestamp = timestamp
        self.delta = delta
        self.description = description
        self.ID = 0

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
        debug_assert(self.ID not in unique_hash_set, "Hash collision A="+str(self.ID))
        unique_hash_set.add(self.ID)

json_register_writeable(Transaction)
json_register_readable(Transaction)

class Account :

    def __init__(self, name : str = "", start_value : float = 0.0, transactions : typing.List[Transaction] = []) :
        self.name = name
        self.start_value = start_value
        self.transactions = transactions

        value = start_value
        for transaction in self.transactions :
            value = value + transaction.delta

        self.end_value = round(value, 2)

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
        debug_assert(self.ID not in unique_hash_set, "Hash collision A="+str(self.ID))
        unique_hash_set.add(self.ID)

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

json_register_writeable(Account)
json_register_readable(Account)

class LedgerEntry :

    def __init__(self, from_account_name : str, from_transaction : Transaction, to_account_name : str, to_transaction : Transaction) :
        self.from_account_name = from_account_name
        self.from_ID = from_transaction.ID
        self.from_value = from_transaction.delta
        self.to_account_name = to_account_name
        self.to_ID = to_transaction.ID
        self.to_value = to_transaction.delta

class LedgerAccount :

    def __init__(self, name : str) :
        pass

class Ledger :

    def __init__(self) :
        self.ID = 0

json_register_writeable(Ledger)
json_register_readable(Ledger)
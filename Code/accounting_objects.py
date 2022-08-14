import typing
import hashlib
from pandas import DataFrame, Series

from debug import debug_assert
from json_file import json_register_writeable, json_register_readable

StringHashMap = typing.Dict[int, str]
PerTypeHashMap = typing.Dict[type, StringHashMap]

class UniqueHashCollector :

    def __init__(self) :
        self.hash_map : PerTypeHashMap = {}

    def register_hash(self, hash_code : int, data_type : type, hash_hint : str) -> None :
        if data_type not in self.hash_map :
            self.hash_map[data_type] = {}

        type_hash_map : StringHashMap = self.hash_map[data_type]
        debug_assert(hash_code not in type_hash_map, "Hash collision! " + str(hash_code) + " from (" + hash_hint + "), existing = (" + type_hash_map.get(hash_code, "ERROR!") + ")")
        type_hash_map[hash_code] = hash_hint

    def get_hasher(self) :
        return hashlib.shake_256()


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

    def update_hash(self, increment : int, hash_collector : UniqueHashCollector) -> None :
        hasher = hash_collector.get_hasher()
        hasher.update(self.date.encode())
        tsNum, tsDen = self.timestamp.as_integer_ratio()
        hasher.update(tsNum.to_bytes(8, 'big', signed=True))
        hasher.update(tsDen.to_bytes(8, 'big'))
        dtNum, dtDen = self.delta.as_integer_ratio()
        hasher.update(dtNum.to_bytes(8, 'big', signed=True))
        hasher.update(dtDen.to_bytes(8, 'big'))
        hasher.update(self.description.encode())
        self.ID = int.from_bytes(hasher.digest(12), 'big')
        self.ID <<= 32 #(4*8) pad 4 bytes
        self.ID += increment
        hash_collector.register_hash(self.ID, Transaction, "Trsctn: time=" + str(self.timestamp))

json_register_writeable(Transaction)
json_register_readable(Transaction)

def get_timestamp(transaction : Transaction) -> float :
    return transaction.timestamp

class Account :

    def __init__(self, name : str = "DEFAULT_ACCOUNT", start_value : float = 0.0, transactions : typing.List[Transaction] = []) :
        self.name : str = name
        self.start_value : float = start_value
        self.transactions : typing.List[Transaction] = []
        self.end_value : float = 0.0
        self.ID : int = 0

        self.__add_transactions(transactions)

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

    def update_hash(self, hash_collector : UniqueHashCollector) -> None :
        hasher = hash_collector.get_hasher()
        hasher.update(self.name.encode())
        svNum, svDen = self.start_value.as_integer_ratio()
        hasher.update(svNum.to_bytes(8, 'big', signed=True))
        hasher.update(svDen.to_bytes(8, 'big'))

        transaction_increment = 0
        for transaction in self.transactions :
            transaction.update_hash(transaction_increment, hash_collector)
            hasher.update(transaction.ID.to_bytes(16, 'big'))
            transaction_increment += 1

        evNum, evDen = self.end_value.as_integer_ratio()
        hasher.update(evNum.to_bytes(8, 'big', signed=True))
        hasher.update(evDen.to_bytes(8, 'big'))
        self.ID = int.from_bytes(hasher.digest(16), 'big')
        hash_collector.register_hash(self.ID, Account, "Acct: name=" + self.name)

    def make_account_data_table(self) -> DataFrame :
        account_data = DataFrame([{ "Date" : t.date, "Delta" : t.delta, "Description" : t.description } for t in self.transactions])
        balance_list = []
        current_balance = self.start_value
        for transaction in self.transactions :
            current_balance += transaction.delta
            balance_list.append(round(current_balance, 2))
        return account_data.join(Series(balance_list, name="Balance"))

    def __add_transactions(self, transactions : typing.List[Transaction]) -> None :

        if len(transactions) > 0 :
            transactions = sorted(transactions, key=get_timestamp)

            value = self.start_value
            for transaction in transactions :
                value += transaction.delta
            self.end_value = round(value, 2)

            self.transactions.extend(transactions)

json_register_writeable(Account)
json_register_readable(Account)

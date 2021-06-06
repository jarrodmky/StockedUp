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

unique_hash_map : typing.Mapping[int, str] = {}

def register_unique_hash(hash_code : int, hash_hint : str) :
    debug_assert(hash_code not in unique_hash_map, "Hash collision! " + str(hash_code) + " from (" + hash_hint + "), existing = (" + unique_hash_map.get(hash_code, "ERROR!") + ")")
    unique_hash_map[hash_code] = hash_hint


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

    def update_hash(self) :
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
        register_unique_hash(self.ID, "Trsctn: time=" + str(self.timestamp))

json_register_writeable(Transaction)
json_register_readable(Transaction)

def sort_id(transaction : Transaction) -> float :
    return transaction.timestamp

class Account :

    def __init__(self, name : str = "", start_value : float = 0.0, transactions : typing.List[Transaction] = []) :
        self.name : str = name
        self.start_value : float = start_value
        self.transactions : typing.List[Transaction] = []
        self.end_value : float = 0.0
        self.ID : int = 0

        self.add_transactions(transactions)

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

    def update_hash(self) :
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
        register_unique_hash(self.ID, "Acct: name=" + self.name)

    def add_transactions(self, transactions : typing.List[Transaction]) :

        #increment timestamps for same day transactions (hash collision prevention)
        if len(transactions) > 0 :
            transactions = sorted(transactions, key=sort_id)

            increment = 0.1
            current_day_stamp = transactions[0].timestamp
            current_increment = increment
            for transaction in transactions[1:] :
                if current_day_stamp == transaction.timestamp :
                    transaction.timestamp += current_increment
                    current_increment += increment
                else :
                    current_day_stamp = transaction.timestamp
                    current_increment = increment

            value = self.start_value
            for transaction in transactions :
                transaction.update_hash()
                value = value + transaction.delta
            self.end_value = round(value, 2)

            self.transactions.extend(transactions)
            self.update_hash()

class AccountDataTable :

    AccountRowType = typing.Tuple[str, float, float, str]

    @staticmethod
    def row(transaction : Transaction, current_balance : float) -> typing.Dict :
        #assume headers as "Date", "Delta", "Balance", "Description"
        return { "Date" : transaction.date, "Delta" : transaction.delta, "Balance" : round(current_balance, 2), "Description" : transaction.description }

    def __init__(self, account : Account) :
        self.name = account.name
        self.row_data : typing.Dict = {}
        current_balance = account.start_value
        index : int = 0
        for transaction in account.transactions :
            current_balance += transaction.delta
            self.row_data[str(index)] = AccountDataTable.row(transaction, current_balance)
            index += 1

    def row_count(self) -> int :
        return len(self.row_data)


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


class AccountManager :

    class AccountDataAndTable :

        def __init__(self, account_data : Account) :
            self.account_data = account_data
            self.account_table_data = AccountDataTable(account_data)


    def __init__(self) :
        loaded_base_accounts = AccountManager.__load_accounts(transaction_base_data_path)

        self.base_account_lookup : typing.Mapping[str, AccountManager.AccountDataAndTable] = {}
        for account in loaded_base_accounts :
            self.base_account_lookup[account.name] = AccountManager.AccountDataAndTable(account)

        loaded_derived_accounts = AccountManager.__load_accounts(transaction_derived_data_path)

        self.derived_account_lookup : typing.Mapping[str, AccountManager.AccountDataAndTable] = {}
        for account in loaded_derived_accounts :
            self.derived_account_lookup[account.name] = AccountManager.AccountDataAndTable(account)

    @staticmethod
    def __load_accounts(directory : pathlib.Path) -> typing.List[Account] :

        account_list = []
        for base_account_file in directory.iterdir() :
            account_list.append(json_read(base_account_file))

        return account_list

    def __get_account_data_pair(self, account_name : str) -> AccountDataAndTable :
        if account_name in self.base_account_lookup :
            return self.base_account_lookup[account_name]
        elif account_name in self.derived_account_lookup :
            return self.derived_account_lookup[account_name]
        else :
            return None

    def get_account_names(self) -> typing.List[str] :
        derived_account_names = list(self.derived_account_lookup.keys())
        base_account_names = list(self.base_account_lookup.keys())
        return sorted(base_account_names + derived_account_names)

    def get_account_data(self, account_name : str) -> Account :
        data_set = self.__get_account_data_pair(account_name)
        if data_set is not None :
            return data_set.account_data
        return None

    def get_account_table(self, account_name : str) -> AccountDataTable :
        data_set = self.__get_account_data_pair(account_name)
        if data_set is not None :
            return data_set.account_table_data
        return None

    def create_derived_account(self, account_name : str) -> bool :
        new_account = Account(account_name)
        new_account.update_hash()
        write_file_path : pathlib.Path = transaction_derived_data_path.joinpath(account_name + ".json")

        if not transaction_derived_data_path.exists() :
            transaction_derived_data_path.mkdir()

        with open(write_file_path, 'x') as output_file :
            pass

        json_write(write_file_path, new_account)
        self.derived_account_lookup[account_name] = AccountManager.AccountDataAndTable(new_account)


#json_register_writeable(Ledger)
#json_register_readable(Ledger)
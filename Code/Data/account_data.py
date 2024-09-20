import typing
from polars import DataFrame, from_dicts
from ..PyJMy.json_file import json_register_writeable, json_register_readable

derived_transaction_columns = ["date", "delta", "description", "timestamp", "source_ID", "source_account"]
unidentified_transaction_columns = ["date", "delta", "description", "timestamp"]
transaction_columns = ["ID", "date", "delta", "description", "timestamp"]
ledger_columns = ["from_account_name", "from_transaction_id", "to_account_name", "to_transaction_id", "delta"]

class Account :

    def __init__(self, name : str = "DEFAULT_ACCOUNT", start_value : float = 0.0, transactions : DataFrame = DataFrame()) :
        self.name : str = name
        self.start_value : float = start_value
        self.transactions : DataFrame = transactions
        self.end_value : float = self.start_value
        if transactions.height > 0 :
            self.end_value = round(self.start_value + sum(self.transactions["delta"]), 2)
            self.end_value = 0.0 if self.end_value == 0.0 else self.end_value #TODO negative zero outputs of sum?
        self.ID : str = "0"

    @staticmethod
    def decode(reader) :
        new_accout = Account()
        new_accout.ID =  reader["ID"]
        new_accout.name = reader["name"]
        new_accout.start_value = reader["start_value"]
        new_accout.end_value = reader["end_value"]
        new_accout.transactions = from_dicts(reader["transactions"])
        return new_accout
    
    def encode(self) :
        writer : typing.Dict[str, typing.Any] = {}
        writer["ID"] = self.ID
        writer["name"] = self.name
        writer["start_value"] = self.start_value
        writer["end_value"] = self.end_value
        writer["transactions"] = self.transactions.to_dicts()
        return writer

json_register_writeable(Account)
json_register_readable(Account)
import typing
from polars import DataFrame, from_dicts

from Code.Utils.json_file import json_register_writeable, json_register_readable


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

    @staticmethod
    def decode(reader) :
        new_accout = Account()
        new_accout.name = reader["name"]
        new_accout.start_value = reader["start_value"]
        new_accout.end_value = reader["end_value"]
        new_accout.transactions = from_dicts(reader["transactions"])
        return new_accout
    
    def encode(self) :
        writer : typing.Dict[str, typing.Any] = {}
        writer["name"] = self.name
        writer["start_value"] = self.start_value
        writer["end_value"] = self.end_value
        writer["transactions"] = self.transactions.to_dicts()
        return writer

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
    
        def encode(self) :
            writer : typing.Dict[str, typing.Any] = {}
            writer["account name"] = self.account_name
            writer["strings"] = self.strings
            return writer

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
    
    def encode(self) :
        writer : typing.Dict[str, typing.Any] = {}
        writer["name"] = self.name
        writer["matchings"] = self.matchings
        writer["starting value"] = self.start_value
        return writer

json_register_readable(DerivedAccount)
json_register_writeable(DerivedAccount)
json_register_readable(DerivedAccount.Matching)
json_register_writeable(DerivedAccount.Matching)

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
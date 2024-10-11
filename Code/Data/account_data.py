import typing
from polars import DataFrame, from_dicts
from Code.json_utils import json_serializer

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
    
    @staticmethod
    def encode(obj) :
        writer : typing.Dict[str, typing.Any] = {}
        writer["name"] = obj.name
        writer["start_value"] = obj.start_value
        writer["end_value"] = obj.end_value
        writer["transactions"] = obj.transactions.to_dicts()
        return writer
    
json_serializer.register_readable(Account)
json_serializer.register_writeable(Account)

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
    
        @staticmethod
        def encode(obj) :
            writer : typing.Dict[str, typing.Any] = {}
            writer["account name"] = obj.account_name
            writer["strings"] = obj.strings
            return writer

    def __init__(self) :
        self.name = "<INVALID ACCOUNT>"
        self.matchings : typing.List[DerivedAccount.Matching] = []
        self.start_value = 0.0

    @staticmethod
    def decode(reader) :
        new_derived_account = DerivedAccount()
        new_derived_account.name = reader["name"]
        new_derived_account.matchings = [DerivedAccount.Matching.decode(m) for m in reader["matchings"]]
        if "starting value" in reader :
            new_derived_account.start_value = reader["starting value"]
        else :
            new_derived_account.start_value = 0.0
        return new_derived_account
    
    @staticmethod
    def encode(obj) :
        writer : typing.Dict[str, typing.Any] = {}
        writer["name"] = obj.name
        writer["matchings"] = [DerivedAccount.Matching.encode(m) for m in obj.matchings]
        writer["starting value"] = obj.start_value
        return writer
    
json_serializer.register_writeable(DerivedAccount)

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
    
    @staticmethod
    def encode(obj) :
        writer : typing.Dict[str, typing.Any] = {}
        writer["from account"] = obj.from_account
        writer["from matchings"] = obj.from_match_strings
        writer["to account"] = obj.to_account
        writer["to matchings"] = obj.to_match_strings
        return writer

class AccountImport :

    def __init__(self) :
        self.account_name : str = "<INVALID ACCOUNT>"
        self.opening_balance : float = 0.0

    @staticmethod
    def decode(reader) :
        new_account_import = AccountImport()
        new_account_import.account_name = reader["account name"]
        if "opening balance" in reader :
            new_account_import.opening_balance = reader["opening balance"]
        else :
            new_account_import.opening_balance = 0.0
        return new_account_import
    
class LedgerImport :

    def __init__(self) :
        self.ledger_name : str = "<INVALID LEDGER>"
        self.accounting_file : str = "<INVALID FILE>"
        self.source_account_folder : str = "INVALID_FOLDER"
        self.raw_accounts : typing.List[AccountImport] = []

    @staticmethod
    def decode(reader) :
        new_ledger_import = LedgerImport()
        new_ledger_import.ledger_name = reader["ledger name"]
        new_ledger_import.accounting_file = reader["accounting file"]
        new_ledger_import.source_account_folder = reader["source account directory"]
        new_ledger_import.raw_accounts = [AccountImport.decode(ra) for ra in reader["source accounts"]]
        return new_ledger_import

class LedgerConfiguration :

    def __init__(self) :
        self.default_ledger : str = "<INVALID LEDGER>"
        self.ledgers : typing.List[LedgerImport] = []

    @staticmethod
    def decode(reader) :
        new_ledger_config = LedgerConfiguration()
        new_ledger_config.default_ledger = reader["default ledger"]
        new_ledger_config.ledgers = [LedgerImport.decode(ledger) for ledger in reader["ledgers"]]
        return new_ledger_config

json_serializer.register_readable(LedgerConfiguration)

class AccountMapping :

    def __init__(self) -> None:
        self.derived_accounts : typing.List[DerivedAccount] = []
        self.internal_transactions : typing.List[InternalTransactionMapping] = []

    @staticmethod
    def decode(reader) :
        new_account_mapping = AccountMapping()
        new_account_mapping.derived_accounts = [DerivedAccount.decode(da) for da in reader["derived accounts"]]
        new_account_mapping.internal_transactions = [InternalTransactionMapping.decode(it) for it in reader["internal transactions"]]
        return new_account_mapping
    
    @staticmethod
    def encode(obj) :
        writer : typing.Dict[str, typing.Any] = {}
        writer["derived accounts"] = [DerivedAccount.encode(da) for da in obj.derived_accounts]
        writer["internal transactions"] = [InternalTransactionMapping.encode(it) for it in obj.internal_transactions]
        return writer
    
json_serializer.register_readable(AccountMapping)
json_serializer.register_writeable(AccountMapping)

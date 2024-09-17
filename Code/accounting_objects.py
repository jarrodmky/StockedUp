import typing

from Code.PyJMy.json_file import json_register_writeable, json_register_readable

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

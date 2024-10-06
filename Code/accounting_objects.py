import typing

from Code.Data.account_data import AccountImport
from Code.Utils.json_file import json_register_readable

class LedgerImport :

    def __init__(self) :
        self.ledger_name : str = "<INVALID LEDGER>"
        self.accounting_file : str = "<INVALID FILE>"
        self.source_account_folder = "<INVALID FOLDER>"
        self.raw_accounts : typing.List[AccountImport] = []

    @staticmethod
    def decode(reader) :
        new_ledger_import = LedgerImport()
        new_ledger_import.ledger_name = reader["ledger name"]
        new_ledger_import.accounting_file = reader["accounting file"]
        new_ledger_import.source_account_folder = reader["source account directory"]
        new_ledger_import.raw_accounts = reader["source accounts"]
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

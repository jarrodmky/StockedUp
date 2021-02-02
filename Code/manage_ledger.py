import pathlib
import typing

from json_file import json_write
from json_file import json_read
from debug import debug_message
from accounting import Account

data_path = pathlib.Path("Data")
transaction_base_data_path = data_path.joinpath("TransactionBase")
transaction_derived_data_path = data_path.joinpath("TransactionDerived")
ledger_data_path = data_path.joinpath("Ledger")

ledger_data_file = ledger_data_path.joinpath("Entries.json")
account_data_file = ledger_data_path.joinpath("Accounts.json")

base_accounts = []
for base_account_file in transaction_base_data_path.iterdir() :
    base_accounts.append(json_read(base_account_file))

if not ledger_data_path.exists() :
    ledger_data_path.mkdir(parents=True)


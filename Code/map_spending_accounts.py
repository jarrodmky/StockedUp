import argparse
import pathlib

from debug import debug_assert
from accounting import Ledger

parser = argparse.ArgumentParser(description="For a certain ledger, performs a mapping of transactions into derived accounts")
parser.add_argument("--data_dir", nargs=1, required=True, help="Root directory for all accounting ledgers and data", metavar="<Data Directory>", dest="data_directory")
parser.add_argument("--ledger", nargs=1, required=True, help="The ledger the accounts are mapped in", metavar="<Ledger Name>", dest="ledger_name")

arguments = parser.parse_args()

debug_assert(isinstance(arguments.data_directory, list) and len(arguments.data_directory) == 1)
data_root_directory = pathlib.Path(arguments.data_directory[0])
data_root_directory = data_root_directory.absolute()
if not data_root_directory.exists() :
    data_root_directory.mkdir()

debug_assert(isinstance(arguments.ledger_name, list) and len(arguments.ledger_name) == 1)
ledger_name = arguments.ledger_name[0]

ledger_data_path = data_root_directory.joinpath(ledger_name)
if not ledger_data_path.exists() or not ledger_data_path.is_dir() :
    raise FileNotFoundError(f"The ledger directory {ledger_data_path} does not exist!")
else :
    ledger = Ledger(ledger_data_path)
    ledger.create_derived_accounts()

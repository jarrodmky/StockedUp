import argparse
import pathlib

from debug import debug_assert, debug_message
from accounting import Ledger

parser = argparse.ArgumentParser(description="Consolidate a batch of csv files to json file representing one account with debits and credits")
parser.add_argument("--data_dir", nargs=1, required=True, help="Root directory for all accounting ledgers and data", metavar="<Data Directory>", dest="data_directory")
parser.add_argument("--ledger", nargs=1, required=True, help="The ledger the account is imported to", metavar="<Ledger Name>", dest="ledger_name")
parser.add_argument("--input", nargs=1, required=True, help="Folder path to CSV files (folder name is account name)", metavar="<Input folder>", dest="input_folder")
parser.add_argument("--open_balance", nargs=1, default=0.0, help="Balance to use for calculation", metavar="<Open Balance>", dest="open_balance")

arguments = parser.parse_args()

debug_assert(isinstance(arguments.data_directory, list) and len(arguments.data_directory) == 1)
data_root_directory = pathlib.Path(arguments.data_directory[0])
data_root_directory = data_root_directory.absolute()
if not data_root_directory.exists() :
    data_root_directory.mkdir()

debug_assert(isinstance(arguments.ledger_name, list) and len(arguments.ledger_name) == 1)
input_ledger_name = arguments.ledger_name[0]

debug_assert(isinstance(arguments.input_folder, list) and len(arguments.input_folder) == 1)
input_folder_path = data_root_directory.joinpath(arguments.input_folder[0])
if not input_folder_path.exists() :
    raise FileNotFoundError(f"Could not find expected filepath {input_folder_path}")

open_balance = 0.0
if isinstance(arguments.open_balance, list) and len(arguments.open_balance) == 1 :
    open_balance = float(arguments.open_balance[0])

ledger_data_path = data_root_directory.joinpath(input_ledger_name)
if not ledger_data_path.exists() :
    debug_message(f"Creating ledger folder {ledger_data_path}")
    ledger_data_path.mkdir()

input_filepaths = []
for file_path in input_folder_path.iterdir() :
    if file_path.is_file() and file_path.suffix == ".csv" :
        input_filepaths.append(file_path)

ledger = Ledger(ledger_data_path)
account_name = input_folder_path.stem
if ledger.account_is_created(account_name) :
    ledger.delete_account(account_name)
ledger.create_account_from_csvs(account_name, input_filepaths, open_balance)

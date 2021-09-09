import argparse
import pathlib

from debug import debug_assert, debug_message
from accounting import Ledger, data_path

parser = argparse.ArgumentParser(description="Consolidate a batch of csv files to json file representing one account with debits and credits")
parser.add_argument("--ledger", nargs=1, required=True, help="The ledger the account is imported to", metavar="<Ledger Name>", dest="ledger_name")
parser.add_argument("--input", nargs=1, required=True, help="Folder path to CSV files (folder name is account name)", metavar="<Input folder>", dest="input_folder")
parser.add_argument("--open_balance", nargs=1, default=0.0, help="Balance to use for calculation", metavar="<Open Balance>", dest="open_balance")
parser.add_argument("--type", nargs=1, default="CU", choices=["CU", "MC", "VISA"], help="Specifies the column format for CSV data", metavar="<CSV Type>", dest="csv_type_string")

arguments = parser.parse_args()

debug_assert(isinstance(arguments.ledger_name, list) and len(arguments.ledger_name) == 1)
input_ledger_name = arguments.ledger_name[0]

debug_assert(isinstance(arguments.input_folder, list) and len(arguments.input_folder) == 1)
input_folder_path = pathlib.Path(arguments.input_folder[0])
input_filepaths = []
for file_path in input_folder_path.iterdir() :
    if file_path.is_file() and file_path.suffix == ".csv" :
        input_filepaths.append(file_path)

open_balance = 0.0
if isinstance(arguments.open_balance, list) and len(arguments.open_balance) == 1 :
    open_balance = float(arguments.open_balance[0])

csv_format = "CU"
if isinstance(arguments.csv_type_string, list) and len(arguments.csv_type_string) == 1 :
    csv_format = arguments.csv_type_string[0]

ledger_data_path = data_path.joinpath(input_ledger_name)
if not ledger_data_path.exists() :
    debug_message(f"Creating ledger folder {input_ledger_name}")
    ledger_data_path.mkdir()

ledger = Ledger(ledger_data_path)
account_name = input_folder_path.stem
if ledger.account_is_created(account_name) :
    ledger.delete_account(account_name)
ledger.create_account_from_csvs(account_name, input_filepaths, open_balance, csv_format)

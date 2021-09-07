import argparse
import pathlib
import typing
import datetime

from json_file import json_write
from debug import debug_message, debug_assert
from accounting_objects import Transaction
from accounting_objects import Account

StringList = typing.List[str]

def check_column_data(column_data : StringList, column_amount : int) :
    debug_message(f"{column_data}")
    debug_assert(len(column_data) == column_amount)

def get_delta_value(debit_value : str, credit_value : str) :
    debit_empty = (debit_value == "")
    debug_assert(debit_empty != (credit_value == ""))

    if debit_empty :
        return -float(credit_value)
    else :
        return float(debit_value)

def read_transaction_MC(column_data : StringList) -> Transaction :
    # [TRANS. DATE / POST DATE / CARD NO. / DESCRIPTION / CATEGORY / DEBIT / CREDIT]
    check_column_data(column_data, 7)

    delta = -get_delta_value(column_data[5], column_data[6]) #swap sign, debit/credit is relative to MC
    description = column_data[3]
    transaction_date = column_data[0]

    time_point = datetime.datetime.strptime(transaction_date, "%Y-%m-%d")
    return Transaction(time_point.strftime("%Y-%m-%d"), time_point.timestamp(), delta, description)

def read_transaction_VISA(column_data : StringList) -> Transaction :
    # [USER / CARD NO. / TRANS. DATE / POST DATE / DESCRIPTION / CURRENCY / DEBIT / CREDIT]
    check_column_data(column_data, 8)

    delta = -get_delta_value(column_data[6], column_data[7]) #swap sign, debit/credit is relative to VISA
    description = column_data[4]
    transaction_date = column_data[2]

    time_point = datetime.datetime.strptime(transaction_date, "%Y-%m-%d")
    return Transaction(time_point.strftime("%Y-%m-%d"), time_point.timestamp(), delta, description)

def read_transaction_CU(column_data : StringList) -> Transaction :
    # [ID / TRANS. DATE / DESCRIPTION / CHEQUE NO. / CREDIT / DEBIT / CURRENT]
    check_column_data(column_data, 7)

    delta = get_delta_value(column_data[5], column_data[4])
    description = column_data[2][1:-1]
    transaction_date = column_data[1]

    time_point = datetime.datetime.strptime(transaction_date, "%d-%b-%Y")
    return Transaction(time_point.strftime("%Y-%m-%d"), time_point.timestamp(), delta, description)

def transactions_from_csvs(input_filepaths : typing.List[pathlib.Path], csv_format : str) -> typing.List[Transaction] :

    if csv_format == "VISA" :
        read_transaction_fxn = read_transaction_VISA
    elif csv_format == "MC" :
        read_transaction_fxn = read_transaction_MC
    else : #default is "CU"
        read_transaction_fxn = read_transaction_CU

    read_transactions = []
    for input_file in input_filepaths :
        with open(input_file, 'r') as read_file :
            for read_line in read_file.readlines() :
                read_transactions.append(read_transaction_fxn(read_line[:-1].split(",")))

    return read_transactions

if __name__ == "__main__" :

    data_path = pathlib.Path("Data")
    if not data_path.exists() :
        data_path.mkdir()

    parser = argparse.ArgumentParser(description="Consolidate a batch of csv files to json file representing one account with debits and credits")
    parser.add_argument("--ledger", nargs=1, required=True, help="The ledger the account is imported to", metavar="<Ledger Name>", dest="ledger_name")
    parser.add_argument("--type", nargs=1, default="CU", choices=["CU", "MC", "VISA"], help="Specifies the column format for CSV data", metavar="<CSV Type>", dest="csv_type_string")
    parser.add_argument("--input", nargs=1, required=True, help="Folder path to CSV files (folder name is account name)", metavar="<Input folder>", dest="input_folder")
    parser.add_argument("--open_balance", nargs=1, default=0.0, help="Balance to use for calculation", metavar="<Open Balance>", dest="open_balance")

    arguments = parser.parse_args()

    debug_assert(isinstance(arguments.ledger_name, list) and len(arguments.ledger_name) == 1)
    input_ledger_name = arguments.ledger_name[0]

    debug_assert(isinstance(arguments.input_folder, list) and len(arguments.input_folder) == 1)
    input_folder_path = pathlib.Path(arguments.input_folder[0])
    input_filepaths = []
    for file_path in input_folder_path.iterdir() :
        if file_path.is_file() and file_path.suffix == ".csv" :
            input_filepaths.append(file_path)

    debug_assert(isinstance(arguments.open_balance, list) and len(arguments.open_balance) == 1)
    open_balance = float(arguments.open_balance[0])

    csv_format = ""
    if isinstance(arguments.csv_type_string, list) and len(arguments.csv_type_string) == 1:
        csv_format = arguments.csv_type_string[0]

    account = Account(input_folder_path.stem, open_balance, transactions_from_csvs(input_filepaths, csv_format))
    ledger_file_path = data_path.joinpath(input_ledger_name)
    account_file_path = ledger_file_path.joinpath("BaseAccounts").joinpath(input_folder_path.stem + ".json")

    if not account_file_path.parent.exists() :
        account_file_path.parent.mkdir(parents=True)

    if account_file_path.exists() :
        account_file_path.unlink()

    with open(account_file_path, 'x') as _ :
        pass

    json_write(account_file_path, account)

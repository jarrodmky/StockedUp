import pathlib
import argparse
import typing
import datetime

from json_file import json_write
from debug import debug_message
from debug import debug_assert
from accounting import Transaction
from accounting import Account


StringList = typing.List[str]

def check_column_data(column_data : StringList, column_amount : int) :
    debug_message(str(column_data))
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

    delta = -get_delta_value(column_data[5], column_data[6]) #swap polarity, debit/credit is relative to MC
    description = column_data[3]
    transaction_date = column_data[0]

    time_point = datetime.datetime.strptime(transaction_date, "%Y-%m-%d")
    transaction = Transaction(transaction_date, time_point.timestamp(), delta, description)
    
    return transaction

def read_transaction_VISA(column_data : StringList) -> Transaction :
    # [USER / CARD NO. / TRANS. DATE / POST DATE / DESCRIPTION / CURRENCY / DEBIT / CREDIT]
    check_column_data(column_data, 8)

    delta = -get_delta_value(column_data[6], column_data[7]) #swap polarity, debit/credit is relative to VISA
    description = column_data[4]
    transaction_date = column_data[2]

    time_point = datetime.datetime.strptime(transaction_date, "%Y-%m-%d")
    transaction = Transaction(transaction_date, time_point.timestamp(), delta, description)
    
    return transaction

def read_transaction_CU(column_data : StringList) -> Transaction :
    # [ID / TRANS. DATE / DESCRIPTION / CHEQUE NO. / CREDIT / DEBIT / CURRENT]
    check_column_data(column_data, 7)

    delta = get_delta_value(column_data[5], column_data[4])
    description = column_data[2][1:-1]
    transaction_date = column_data[1]

    time_point = datetime.datetime.strptime(transaction_date, "%d-%b-%Y")
    return Transaction(transaction_date, time_point.timestamp(), delta, description)

def sort_id(transaction : Transaction) -> float :
    return transaction.timestamp


parser = argparse.ArgumentParser(description="Consolidate a batch of csv files to json file representing one account with debits and credits")
parser.add_argument("--type", nargs=1, default="CU", choices=["CU", "MC", "VISA"], help="Specifies the column format for CSV data", metavar="<CSV Type>", dest="csv_type_string")
parser.add_argument("--input", nargs='+', required=True, help="File paths to read", metavar="<Input file>", dest="input_files")
parser.add_argument("--output", nargs=1, required=True, help="File name for account JSON", metavar="<Output name>", dest="output_file")
parser.add_argument("--open_balance", nargs=1, default=0.0, help="Balance to use for calculation", metavar="<Open Balance>", dest="open_balance")

arguments = parser.parse_args()

data_path = pathlib.Path("Data")
transaction_base_data_path = data_path.joinpath("TransactionBase")

debug_assert(isinstance(arguments.output_file, list) and len(arguments.output_file) == 1)
account_name = arguments.output_file[0]
output_filepath = transaction_base_data_path.joinpath(account_name + ".json")

debug_assert(isinstance(arguments.input_files, list) and len(arguments.input_files) > 0)
input_filepaths = []
for file_string in arguments.input_files :
    input_filepaths.append(data_path.joinpath(file_string))

debug_assert(isinstance(arguments.open_balance, list) and len(arguments.open_balance) == 1)
open_balance = float(arguments.open_balance[0])

csv_format = ""
if isinstance(arguments.csv_type_string, list) and len(arguments.csv_type_string) == 1:
    csv_format = arguments.csv_type_string[0]

if csv_format == "VISA" :
    read_transaction = read_transaction_VISA
elif csv_format == "MC" :
    read_transaction = read_transaction_MC
else : #default is "CU"
    read_transaction = read_transaction_CU

read_transactions = []
for input_file in input_filepaths :
    with open(input_file, 'r') as read_file :
        for read_line in read_file.readlines() :
            read_transactions.append(read_transaction(read_line[:-1].split(",")))
            
read_transactions = sorted(read_transactions, key=sort_id)

#increment timestamps for same day transactions (hash collision prevention)
if len(read_transactions) > 0 :
    increment = 0.1
    current_day_stamp = read_transactions[0].timestamp
    current_increment = increment
    for transaction in read_transactions[1:] :
        if current_day_stamp == transaction.timestamp :
            transaction.timestamp += current_increment
            current_increment += increment
        else :
            current_day_stamp = transaction.timestamp
            current_increment = increment

    for transaction in read_transactions :
        transaction.hash_internal()

account = Account(account_name, open_balance, read_transactions)
account.hash_internal()

if not transaction_base_data_path.exists() :
    transaction_base_data_path.mkdir(parents=True)

if output_filepath.exists() :
    output_filepath.unlink()

with open(output_filepath, 'x') as output_file :
    pass

json_write(output_filepath, account)

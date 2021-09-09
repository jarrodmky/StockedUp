import pathlib
import typing
import datetime

from debug import debug_message, debug_assert
from accounting_objects import Transaction

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

def read_transactions_from_csvs(input_filepaths : typing.List[pathlib.Path], csv_format : str) -> typing.List[Transaction] :

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

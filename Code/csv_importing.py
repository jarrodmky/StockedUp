from math import isnan
import pathlib
import typing
import datetime
import pandas

from PyJMy.debug import debug_message, debug_assert
from accounting_objects import Transaction

StringList = typing.List[str]
DeltaFunc = typing.Callable[[float, float], float]

def check_formats_convertible(from_data_type : pandas.Series, to_data_type : pandas.Series) -> bool :
    if len(from_data_type) == len(to_data_type) :
        for from_field, to_field in zip(from_data_type, to_data_type) :
            if from_field != to_field :
                if from_field == "Int64" and to_field == "Float64" :
                    #pandas will read integer-valued decimals as Int64 dtype
                    continue
                debug_message(f"Incompatible types: {from_data_type} -x-> {to_data_type}")
                return False
        debug_message(f"Compatible types: {from_data_type} ---> {to_data_type}")
        return True
    else :
        return False

def get_delta_value(credit_value : float, debit_value : float) -> float :
    debit_empty = isnan(debit_value)
    debug_assert(debit_empty != isnan(credit_value))

    if debit_empty :
        return -credit_value
    else :
        return debit_value

def convert_transactions(format_type : typing.Any, transation_data_frame : pandas.DataFrame) -> typing.List[Transaction] :
    transation_data_frame.columns = format_type.column_list
    return [(format_type.make_transaction(data)) for _, data in transation_data_frame.iterrows()]

class CsvFormat_Simple :

    column_format = pandas.Series(["string", "string", "Float64", "Float64", "Int64"])

    column_list = ["TransDate", "Description", "Credit", "Debit", "CardNo"]

    @staticmethod
    def make_transaction(data : pandas.Series) -> Transaction :
        delta = get_delta_value(data.Credit, data.Debit)
        time_point = datetime.datetime.strptime(data.TransDate, "%Y-%m-%d")
        return Transaction(time_point.strftime("%Y-%m-%d"), time_point.timestamp(), delta, data.Description)

class CsvFormat_Cheques :

    column_format = pandas.Series(["string", "string", "string", "Int64", "Float64", "Float64", "Float64"])

    column_list = ["ID", "TransDate", "Description", "ChequeNo", "Credit", "Debit", "Current"]

    @staticmethod
    def make_transaction(data : pandas.Series) -> Transaction :
        delta = get_delta_value(data.Credit, data.Debit)
        time_point = datetime.datetime.strptime(data.TransDate, "%d-%b-%Y")
        return Transaction(time_point.strftime("%Y-%m-%d"), time_point.timestamp(), delta, data.Description)

class CsvFormat_Category :

    column_format = pandas.Series(["string", "string", "Int64", "string", "string", "Float64", "Float64"])

    column_list = ["TransDate", "PostDate", "CardNo", "Description", "Category", "Credit", "Debit"]

    @staticmethod
    def make_transaction(data : pandas.Series) -> Transaction :
        delta = get_delta_value(data.Credit, data.Debit)
        time_point = datetime.datetime.strptime(data.TransDate, "%Y-%m-%d")
        return Transaction(time_point.strftime("%Y-%m-%d"), time_point.timestamp(), delta, data.Description)

class CsvFormat_Detailed :

    column_format = pandas.Series(["string", "Int64", "string", "string", "string", "string", "Float64", "Float64"])

    column_list = ["User", "CardNo", "TransDate", "PostDate", "Description", "Currency", "Credit", "Debit"]

    @staticmethod
    def make_transaction(data : pandas.Series) -> Transaction :
        delta = get_delta_value(data.Credit, data.Debit)
        time_point = datetime.datetime.strptime(data.TransDate, "%Y-%m-%d")
        return Transaction(time_point.strftime("%Y-%m-%d"), time_point.timestamp(), delta, data.Description)

def read_transactions_from_csvs(input_filepaths : typing.List[pathlib.Path]) -> typing.List[Transaction] :

    read_transaction_list = []
    for input_file in input_filepaths :
        debug_message(f"Reading in {input_file}")
        column_data = pandas.read_csv(input_file, header=None)
        column_types = column_data.convert_dtypes().dtypes

        if check_formats_convertible(column_types, CsvFormat_Simple.column_format) :
            read_transaction_list.extend(convert_transactions(CsvFormat_Simple, column_data))
        elif check_formats_convertible(column_types, CsvFormat_Cheques.column_format) :
            read_transaction_list.extend(convert_transactions(CsvFormat_Cheques, column_data))
        elif check_formats_convertible(column_types, CsvFormat_Category.column_format) :
            read_transaction_list.extend(convert_transactions(CsvFormat_Category, column_data))
        elif check_formats_convertible(column_types, CsvFormat_Detailed.column_format) :
            read_transaction_list.extend(convert_transactions(CsvFormat_Detailed, column_data))
        else :
            assert False, "Format not recognized!"

    return read_transaction_list

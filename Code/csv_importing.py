from math import isnan
import pathlib
import typing
import datetime
import pandas

from PyJMy.debug import debug_message, debug_assert

unidentified_transaction_columns = ["date", "delta", "description", "timestamp"]

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

def get_delta_values(credit_values : pandas.Series, debit_values : pandas.Series) -> pandas.Series :
    delta_from_row = lambda row : get_delta_value(row.credit_values, row.debit_value)
    return pandas.DataFrame({"credit_values" : credit_values, "debit_value" : debit_values}).apply(delta_from_row, axis=1, result_type="reduce")

def date_to_date(input_series : pandas.Series, input_format : str, output_format : str) -> pandas.Series :
    return input_series.apply(lambda data : datetime.datetime.strptime(data, input_format).strftime(output_format))

def date_to_timestamp(input_series : pandas.Series, input_format : str) -> pandas.Series :
    return input_series.apply(lambda data : datetime.datetime.strptime(data, input_format).timestamp())

class CsvFormat_Simple :

    column_format = pandas.Series(["string", "string", "Float64", "Float64", "Int64"])

    column_list = ["TransDate", "Description", "Credit", "Debit", "CardNo"]

    @staticmethod
    def read_csv_to_frame(data : pandas.DataFrame) -> pandas.DataFrame :
        input_date_format = "%Y-%m-%d"
        return pandas.DataFrame({
            "date" : date_to_date(data.TransDate, input_date_format, "%Y-%m-%d"),
            "delta" : get_delta_values(data.Credit, data.Debit),
            "description" : data.Description,
            "timestamp" : date_to_timestamp(data.TransDate, input_date_format)
            })

class CsvFormat_Cheques :

    column_format = pandas.Series(["string", "string", "string", "Int64", "Float64", "Float64", "Float64"])

    column_list = ["ID", "TransDate", "Description", "ChequeNo", "Credit", "Debit", "Current"]

    @staticmethod
    def read_csv_to_frame(data : pandas.DataFrame) -> pandas.DataFrame :
        input_date_format = "%d-%b-%Y"
        return pandas.DataFrame({
            "date" : date_to_date(data.TransDate, input_date_format, "%Y-%m-%d"),
            "delta" : get_delta_values(data.Credit, data.Debit),
            "description" : data.Description,
            "timestamp" : date_to_timestamp(data.TransDate, input_date_format)
            })

class CsvFormat_Category :

    column_format = pandas.Series(["string", "string", "Int64", "string", "string", "Float64", "Float64"])

    column_list = ["TransDate", "PostDate", "CardNo", "Description", "Category", "Credit", "Debit"]

    @staticmethod
    def read_csv_to_frame(data : pandas.DataFrame) -> pandas.DataFrame :
        input_date_format = "%Y-%m-%d"
        return pandas.DataFrame({
            "date" : date_to_date(data.TransDate, input_date_format, "%Y-%m-%d"),
            "delta" : get_delta_values(data.Credit, data.Debit),
            "description" : data.Description,
            "timestamp" : date_to_timestamp(data.TransDate, input_date_format)
            })

class CsvFormat_Detailed :

    column_format = pandas.Series(["string", "Int64", "string", "string", "string", "string", "Float64", "Float64"])

    column_list = ["User", "CardNo", "TransDate", "PostDate", "Description", "Currency", "Credit", "Debit"]

    @staticmethod
    def read_csv_to_frame(data : pandas.DataFrame) -> pandas.DataFrame :
        input_date_format = "%Y-%m-%d"
        return pandas.DataFrame({
            "date" : date_to_date(data.TransDate, input_date_format, "%Y-%m-%d"),
            "delta" : get_delta_values(data.Credit, data.Debit),
            "description" : data.Description,
            "timestamp" : date_to_timestamp(data.TransDate, input_date_format)
            })
    

def read_transactions_from_csv_in_path(input_folder_path : pathlib.Path) -> pandas.DataFrame :
    assert input_folder_path.is_dir()
    empty_frame = pandas.DataFrame({
            "date" : pandas.Series(),
            "delta" : pandas.Series(),
            "description" : pandas.Series(),
            "timestamp" : pandas.Series()
            })
    data_frame_list = [empty_frame]
    for file_path in input_folder_path.iterdir() :
        if file_path.is_file() and file_path.suffix == ".csv" :
            data_frame_list.append(read_transactions_from_csv(file_path))
    return pandas.concat(data_frame_list, ignore_index=True)

def read_transactions_from_csv(input_file : pathlib.Path) -> pandas.DataFrame :

    assert input_file.suffix == ".csv"
    debug_message(f"Reading in {input_file}")
    column_data = pandas.read_csv(input_file, header=None)
    column_types = column_data.convert_dtypes().dtypes

    if check_formats_convertible(column_types, CsvFormat_Simple.column_format) :
        ts = CsvFormat_Simple.read_csv_to_frame(pandas.read_csv(input_file, header=None, names=CsvFormat_Simple.column_list))
    elif check_formats_convertible(column_types, CsvFormat_Cheques.column_format) :
        ts = CsvFormat_Cheques.read_csv_to_frame(pandas.read_csv(input_file, header=None, names=CsvFormat_Cheques.column_list))
    elif check_formats_convertible(column_types, CsvFormat_Category.column_format) :
        ts = CsvFormat_Category.read_csv_to_frame(pandas.read_csv(input_file, header=None, names=CsvFormat_Category.column_list))
    elif check_formats_convertible(column_types, CsvFormat_Detailed.column_format) :
        ts = CsvFormat_Detailed.read_csv_to_frame(pandas.read_csv(input_file, header=None, names=CsvFormat_Detailed.column_list))
    else :
        assert False, f"Format not recognized! File {input_file}\n Types :\n {column_types}"

    debug_assert(ts.columns.to_list() == unidentified_transaction_columns)
    return ts

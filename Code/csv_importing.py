import typing
from pathlib import Path
from polars import Series, DataFrame
from polars import when, concat
from polars import String, Float64

from PyJMy.debug import debug_message

def get_default_globals() :
    global_defs = globals()
    default_globals = {}

    for definition_key in list(global_defs.keys()) :
        if definition_key[2:-2] == definition_key.replace("__", "") :
            default_globals[definition_key] = global_defs[definition_key]

def read_dynamic_function(import_function_file : Path, function_name : str) -> typing.Callable :
    assert import_function_file.suffix == ".py"
    script_string = ""

    try :
        with open(import_function_file, 'r') as file:
            script_string = file.read()
        code_object = compile(script_string, import_function_file, 'exec')

        local_defs : typing.Dict[str, typing.Any] = {}
        exec(code_object, get_default_globals(), local_defs)
    
        if function_name in local_defs :
            return local_defs[function_name]
    except Exception as e :
        debug_message(f"Exception when importing function {e}")
    return lambda x : x
    
def read_transactions_from_csv_in_path(input_folder_path : Path) -> DataFrame :
    assert input_folder_path.is_dir()
    empty_frame = DataFrame(schema={
            "date" : String,
            "delta" : Float64,
            "description" : String,
            "timestamp" : Float64
            })
    data_frame_list = [empty_frame]
    import_script = None
    for file_path in input_folder_path.iterdir() :
        if file_path.is_file() and file_path.name == "import_dataframe.py" :
            import_script = file_path
            break
    
    if import_script is None :
        import_dataframe = lambda _ : DataFrame()
    else :
        import_dataframe = read_dynamic_function(import_script, "import_dataframe")
    
    for file_path in input_folder_path.iterdir() :
        if file_path.is_file() and file_path.suffix == ".csv" :
            debug_message(f"Reading in {file_path}")
            try :
                imported_csv = import_dataframe(file_path)
                homogenized_df = homogenize_transactions(imported_csv)
                data_frame_list.append(homogenized_df)
            except Exception as e :
                debug_message(f"Exception importing {file_path}: {e}")

    read_transactions = concat(data_frame_list)
    read_transactions = read_transactions.sort(by="timestamp")
    return read_transactions

def get_delta_values(credit_values : Series, debit_values : Series) -> Series :
    df = DataFrame({"credit_values" : credit_values, "debit_value" : debit_values})
    df = df.with_columns(
        when(df["debit_value"].is_null())
        .then(-df["credit_values"])
        .otherwise(df["debit_value"])
        .alias("result")
        )
    return df["result"]

def homogenize_transactions(df : DataFrame) -> DataFrame :
    return DataFrame({
        "date" : df["TransDate"].dt.to_string("%Y-%m-%d"),
        "delta" : get_delta_values(df["Credit"].cast(Float64), df["Debit"].cast(Float64)),
        "description" : df["Description"],
        "timestamp" : df["TransDate"].dt.epoch(time_unit="s").cast(Float64)
        })

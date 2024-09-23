import typing
from pathlib import Path
from polars import Series, DataFrame
from polars import when, concat
from polars import String, Float64
from prefect import flow, task

from Code.Data import AccountSerializer
from Code.Data.account_data import unidentified_transaction_columns, transaction_columns, Account, AccountImport
from Code.Data.hashing import make_identified_transaction_dataframe, hash_path, hash_float, hash_task_source, hash_string
from Code.accounting_objects import LedgerConfiguration
from Code.Utils.json_file import json_read

from xxhash import xxh64

from Code.logger import get_logger
logger = get_logger(__name__)

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
        with open(import_function_file, 'r') as file :
            script_string = file.read()
        code_object = compile(script_string, import_function_file, 'exec')

        local_defs : typing.Dict[str, typing.Any] = {}
        exec(code_object, get_default_globals(), local_defs)
    
        if function_name in local_defs :
            return local_defs[function_name]
    except Exception as e :
        logger.error(f"Exception when importing function {e}")
    return lambda x : x

def get_import_function(input_folder_path : Path) -> typing.Callable :
    import_script = None
    for file_path in input_folder_path.iterdir() :
        if file_path.is_file() and file_path.name == "import_dataframe.py" :
            import_script = file_path
            break
    
    if import_script is None :
        return lambda _ : DataFrame()
    else :
        return read_dynamic_function(import_script, "import_dataframe")

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

def read_transactions_from_csv(input_file_path : Path) -> DataFrame :
    result = DataFrame()
    if input_file_path.is_file() and input_file_path.suffix == ".csv" :
        logger.info(f"Reading in {input_file_path}")
        import_dataframe = get_import_function(input_file_path.parent)
        imported_csv = import_dataframe(input_file_path)
        result = homogenize_transactions(imported_csv)
    return result

def read_transactions_from_csv_in_path(input_folder_path : Path) -> DataFrame :
    assert input_folder_path.is_dir(), f"invalid directory {input_folder_path}"
    empty_frame = DataFrame(schema={
            "date" : String,
            "delta" : Float64,
            "description" : String,
            "timestamp" : Float64
            })
    assert empty_frame.columns == unidentified_transaction_columns
    data_frame_list = [empty_frame]
    
    for file_path in input_folder_path.iterdir() :
        homogenized_df = read_transactions_from_csv(file_path)
        if homogenized_df.columns == unidentified_transaction_columns :
            data_frame_list.append(homogenized_df)

    read_transactions = concat(data_frame_list)
    read_transactions = read_transactions.sort(by="timestamp")
    return read_transactions

def import_raw_account_key(run_context, parameters) :
    hasher = xxh64()
    hash_string(hasher, parameters["account_name"])
    hash_path(hasher, parameters["raw_account_path"])
    hash_float(hasher, parameters["start_balance"])
    hash_task_source(hasher, run_context)
    return hasher.hexdigest()

@task(
        result_storage_key="{parameters[account_name]}.json", 
        cache_key_fn=import_raw_account_key, 
        result_serializer=AccountSerializer()
        )
def import_raw_account(account_name : str, raw_account_path : Path, start_balance : float) -> Account :
    read_transactions = read_transactions_from_csv_in_path(raw_account_path)
    read_transactions = make_identified_transaction_dataframe(read_transactions)
    assert read_transactions.columns == transaction_columns
    account = Account(account_name, start_balance, read_transactions)
    return account

def import_raw_accounts(account_imports : typing.List[AccountImport], dataroot_path : Path) -> typing.List[Account] :
    imported_accounts : typing.List[Account] = []
    for account_import in account_imports :
        raw_account_path = dataroot_path / account_import.folder
        account = import_raw_account(raw_account_path.stem, raw_account_path, account_import.opening_balance)
        imported_accounts.append(account)
    return imported_accounts

@flow
def import_ledger_source_accounts(ledger_config_path : Path, dataroot_path : Path, ledger_name : str) -> typing.List[Account] :
    account_imports = []
    ledger_config = json_read(ledger_config_path)
    assert isinstance(ledger_config, LedgerConfiguration), "Ledger config not a LedgerConfiguration?"
    for ledger_import in ledger_config.ledgers :
        if ledger_import.name == ledger_name :
            account_imports = ledger_import.raw_accounts
            break

    return import_raw_accounts(account_imports, dataroot_path)

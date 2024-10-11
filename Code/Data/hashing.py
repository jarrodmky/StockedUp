import typing
from xxhash import xxh128, xxh64
from pathlib import Path
from inspect import getsource
from polars import Series, DataFrame, concat, String

from Code.json_utils import json_serializer

def hash_float(hasher : typing.Any, float_number : float) -> None :
    num, den = float_number.as_integer_ratio()
    hasher.update(num.to_bytes(8, 'big', signed=True))
    hasher.update(den.to_bytes(8, 'big'))

def hash_string(hasher : typing.Any, string : str) -> None :
    hasher.update(string.encode())

def hash_object(hasher : typing.Any, some_object : typing.Any) -> None :
    hasher.update(json_serializer.write_to_string(some_object).encode("utf-8"))

def hash_file(hasher : typing.Any, file_path : Path) -> None :
    assert file_path.is_file()
    file_stat = file_path.stat()
    hash_float(hasher, file_stat.st_mtime)
    hash_float(hasher, file_stat.st_ctime)
    hasher.update(file_stat.st_size.to_bytes(8))

def hash_path(hasher : typing.Any, path : Path) -> None :
    if path.is_file() :
        hash_file(hasher, path)
    elif path.is_dir() :
        path_objects = sorted(path.iterdir())
        for subpath in path_objects :
            hash_path(hasher, subpath)

def hash_source(hasher : typing.Any, source_object : typing.Any) -> None :
    source = getsource(source_object)
    hash_string(hasher, source)

def transaction_hash(index : int, date : str, timestamp : float, delta : float, description : str) -> str :
    hasher = xxh128()
    hasher.update(index.to_bytes(8))
    hasher.update(date.encode())
    hash_float(hasher, timestamp)
    hash_float(hasher, delta)
    hasher.update(description.encode())
    return str(hasher.hexdigest())

def make_identified_transaction_dataframe(transactions : DataFrame) -> DataFrame :
    if len(transactions) > 0 :
        index = DataFrame(Series("TempIndex", range(0, transactions.height)))
        indexed_transactions = concat([index, transactions], how="horizontal")
        make_id = lambda t : transaction_hash(int(t[0]), t[1], t[4], t[2], t[3])
        id_frame = indexed_transactions.map_rows(make_id, String)
        id_frame.columns = ["ID"]
    else :
        id_frame = DataFrame(schema={"ID" : String})
    return concat([id_frame, transactions], how="horizontal")

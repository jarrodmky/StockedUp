import typing
from os import walk
from hashlib import sha256
from xxhash import xxh128
from pathlib import Path
from polars import Series, DataFrame, concat, String

class UniqueHashCollector :

    def __init__(self) :
        self.__hash_map : typing.Dict[str, typing.Dict[str, str]] = {}

    def register_hash(self, name_space : str, hash_code : str, hash_hint : str) -> None :
        if name_space not in self.__hash_map :
            self.__hash_map[name_space] = {}

        type_hash_map : typing.Dict[str, str] = self.__hash_map[name_space]
        assert hash_code not in type_hash_map, "Hash collision! " + hash_code + " from (" + hash_hint + "), existing = (" + type_hash_map.get(hash_code, "ERROR!") + ")"
        type_hash_map[hash_code] = hash_hint

def hash_float(hasher : typing.Any, float_number : float) -> None :
    num, den = float_number.as_integer_ratio()
    hasher.update(num.to_bytes(8, 'big', signed=True))
    hasher.update(den.to_bytes(8, 'big'))

def transaction_hash(index : int, date : str, timestamp : float, delta : float, description : str) -> str :
    hasher = xxh128()
    hasher.update(index.to_bytes(8))
    hasher.update(date.encode())
    hash_float(hasher, timestamp)
    hash_float(hasher, delta)
    hasher.update(description.encode())
    return str(hasher.hexdigest())

def file_hash(hasher : typing.Any, file_path : Path) -> None :
    buffer_size = (2 ** 20)
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(buffer_size)
            if not data:
                break
            hasher.update(data)

def folder_csvs_hash(hasher : typing.Any, folder_path : Path) -> None :
    for dirpath, _, filenames in walk(folder_path, onerror=print) :
        for filename in filenames :
            current_file = Path(dirpath) / filename
            if current_file.suffix == ".csv" :
                file_hash(hasher, current_file)

def raw_account_data_hash(folder_path : Path, number : float) -> int :
    if not folder_path.exists() or not folder_path.is_dir() :
        return 0

    sha256_hasher = sha256()
    folder_csvs_hash(sha256_hasher, folder_path)
    hash_float(sha256_hasher, number)
    return int(sha256_hasher.hexdigest(), 16)

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

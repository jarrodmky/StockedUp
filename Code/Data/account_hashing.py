from xxhash import xxh128
from polars import Series, DataFrame, concat, String

from Code.Utils.hashing import hash_float

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
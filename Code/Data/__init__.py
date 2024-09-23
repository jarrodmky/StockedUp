import typing
from polars import from_dicts
from prefect.serializers import Serializer, Literal

from Code.Data.account_data import Account

class AccountSerializer(Serializer) :
    
    type: Literal["Account"] = "Account"

    def dumps(self, data: typing.Any) -> bytes:
        from json import dumps as json_dumps
        obj_dict : typing.Dict[str, typing.Any] = {}
        obj_dict["name"] = data.name
        obj_dict["start_value"] = data.start_value
        obj_dict["end_value"] = data.end_value
        obj_dict["transactions"] = data.transactions.to_dicts()
        return json_dumps(obj_dict, indent=2).encode("utf-8-sig")

    def loads(self, blob: bytes) -> typing.Any:
        from json import loads as json_loads
        reader = json_loads(blob.decode("utf-8-sig"))
        new_accout = Account()
        new_accout.name = reader["name"]
        new_accout.start_value = reader["start_value"]
        new_accout.end_value = reader["end_value"]
        new_accout.transactions = from_dicts(reader["transactions"])
        return new_accout
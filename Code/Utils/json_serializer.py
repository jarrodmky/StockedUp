import json
import typing
from pathlib import Path as FilePath

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

class __json_serializer :

	def __init__(self) -> None:
		self.__writeable_registry : typing.Set[typing.Type] = set()
		self.__readable_registry : typing.Set[typing.Type] = set()

	def register_writeable(self, writeable_type : typing.Type) -> None :
		self.__writeable_registry.add(writeable_type)

	def register_readable(self, readable_type : typing.Type) -> None :
		self.__readable_registry.add(readable_type)

	def write_to_string(self, something : typing.Any) -> str :
		try :
			if type(something) in self.__writeable_registry :
				return json.dumps(something, default=type(something).encode, indent=2, sort_keys=True)
			else :
				return json.dumps(something, indent=2, sort_keys=True)
		except Exception as e :
			logger.error(f"Failed to serialize {type(something)} : {e}")
			return "{}"

	def write_to_file(self, file_path : FilePath, something : typing.Any) -> None :
		try :
			with open(file_path, "w", encoding="utf-8-sig") as write_file :
				if type(something) in self.__writeable_registry :
					json.dump(something, write_file, default=type(something).encode, indent=2, sort_keys=True)
				else :
					json.dump(something, write_file, indent=2, sort_keys=True)
		except Exception as e :
			logger.error(f"Failed to write {file_path} as {type(something)} : {e}")

	def read_from_file(self, file_path : FilePath, read_type : typing.Type = typing.Dict) -> typing.Any :
		try :
			with open(file_path, "r", encoding="utf-8-sig") as read_file :
				if read_type is typing.Dict or read_type not in self.__readable_registry :
					return json.load(read_file)
				else :
					read_object = read_type.decode(json.load(read_file))
					assert isinstance(read_object, read_type), f"Failed to deserialize {str(read_type)}! Check decoding"
					return read_object
		except Exception as e :
			logger.error(f"Failed to read {file_path} as {read_type} : {e}")
		return None

json_serializer = __json_serializer()

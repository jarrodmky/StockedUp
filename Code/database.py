import typing
from pathlib import Path
from hashlib import sha256
from polars import DataFrame, read_database
from sqlalchemy import create_engine, inspect, text
from Code.Utils.json_serializer import json_serializer

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

data_chunk_max = (2 ** 8) * (1024 ** 2)

def get_dataframe_hash(dataframe : DataFrame) -> int :
    sha256_hasher = sha256()
    sha256_hasher.update(dataframe.to_pandas().encode('utf-8'))
    return int(sha256_hasher.hexdigest(), 16)

class SQLDataBase :

    def __init__(self, root_path : Path, name : str) :
        self.__dbfile_path = root_path.joinpath(f"{name}.db")

        self.URI = f"sqlite:///{str(self.__dbfile_path)}"
        self.engine = create_engine(self.URI)

    def store(self, name : str, dataframe : DataFrame) -> bool :
        try :
            logger.info(not self.is_stored(name), "Dataframe is stored!")
            total_memory_needed = dataframe.estimated_size()
            assert total_memory_needed <= data_chunk_max, "Exceeds current allowable dataframe size!"

            dataframe.write_database(name, self.URI)
            return True
        except Exception as e :
            logger.error(f"Tried to store table {name} to {str(self.__dbfile_path)} but hit :\n{e}")
            return False
        
    def update(self, name : str, dataframe : DataFrame) -> None :
        if self.is_stored(name) :
            total_memory_needed = dataframe.estimated_size()
            assert total_memory_needed <= data_chunk_max, "Exceeds current allowable dataframe size!"
            dataframe.write_database(name, self.URI, if_table_exists="replace")
        else :
            self.store(name, dataframe)
    
    def query(self, sql_query : str) -> DataFrame :
        return read_database(sql_query, self.engine)

    def is_stored(self, name : str) -> bool :
        
        inspection = inspect(self.engine)
        return inspection.has_table(name)

    def retrieve(self, name : str) -> DataFrame :
        assert self.is_stored(name), "Cannot find table {file_path}"
        try :
            dataframe = read_database(f"SELECT * FROM {name}", self.engine)
            assert dataframe is not None
            return dataframe
        except Exception as e :
            logger.error(f"Tried to get table {name} but hit :\n{e}")
            return DataFrame()
    
    def drop(self, name : str) -> bool :
        if self.is_stored(name) :
            with self.engine.connect() as connection :
                connection.execute(text(f"DROP TABLE {name}"))
                connection.commit()
            return True
        return False

class JsonDataBase :

    def __init__(self, root_path : Path, name : str) :
        self.__dbfile_path = root_path.joinpath(name)
        if not self.__dbfile_path.exists() :
            self.__dbfile_path.mkdir()

    def store(self, name : str, some_object : typing.Any) -> bool :
        assert not self.is_stored(name), "Dataframe is stored!"
        file_path = self.__get_json_file_path(name)
        try :
            bytestring = json_serializer.write_to_string(some_object).encode("utf-8")
            
            total_memory_needed = len(bytestring)
            assert total_memory_needed <= data_chunk_max, "Exceeds current allowable dataframe size!"

            with open(file_path, 'x') as _ :
                pass
            json_serializer.write_to_file(file_path, some_object)
            return True
        except Exception as e :
            logger.error(f"Tried to store file {file_path} but hit :\n{e}")
            return False
        
    def update(self, name : str, some_object : typing.Any) -> None :
        if self.is_stored(name) :
            file_path = self.__get_json_file_path(name)
            json_serializer.write_to_file(file_path, some_object)
        else :
            self.store(name, some_object)

    def is_stored(self, name : str) -> bool :
        file_path = self.__get_json_file_path(name)
        return file_path.exists() and file_path.is_file()
    
    def __get_json_file_path(self, name : str) -> Path :
        return self.__dbfile_path.joinpath(f"{name}.json")

    def retrieve(self, name : str, object_type : typing.Type = typing.Dict) -> typing.Any :
        assert self.is_stored(name), f"Cannot find object {name}"
        try :
            file_path = self.__get_json_file_path(name)
            some_object = json_serializer.read_from_file(file_path, object_type)
            return some_object
        except Exception as e :
            logger.error(f"Tried to get file {file_path} but hit :\n{e}")
            return None
        
    def get_names(self) :
        name_list = []
        for folder_entry in self.__dbfile_path.iterdir() :
            if folder_entry.is_file() and folder_entry.suffix == ".json" :
                name_list.append(folder_entry.stem)
            else :
                logger.info(f"Found non-database folder entry \"{folder_entry}\"")
        return sorted(name_list)
    
    def drop(self, name : str) -> bool :
        if self.is_stored(name) :
            file_path = self.__get_json_file_path(name)
            file_path.unlink()
            return True
        return False

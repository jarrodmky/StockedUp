import typing
from pathlib import Path
from json import dumps as to_json_string
from PyJMy.debug import debug_message
from PyJMy.json_file import json_read, json_write

data_chunk_max = (2 ** 8) * (1024 ** 2)

class JsonDataBase :

    def __init__(self, root_path : Path, name : str) :
        self.__dbfile_path = root_path.joinpath(name)
        if not self.__dbfile_path.exists() :
            self.__dbfile_path.mkdir()

    def store(self, name : str, some_object : typing.Any) -> bool :
        from PyJMy.json_file import json_encoder
        file_path = self.__get_json_file_path(name)
        try :
            assert not self.is_stored(name), "Dataframe is stored!"
            json_string = to_json_string(some_object, cls=json_encoder).encode("utf-8")
            
            total_memory_needed = len(json_string)
            assert total_memory_needed <= data_chunk_max, "Exceeds current allowable dataframe size!"

            with open(file_path, 'x') as _ :
                pass
            json_write(file_path, some_object)
            return True
        except Exception as e :
            print(f"Tried to store file {file_path} but hit :\n{e}")
            return False

    def is_stored(self, name : str) -> bool :
        file_path = self.__get_json_file_path(name)
        return file_path.exists() and file_path.is_file()
    
    def __get_json_file_path(self, name : str) -> Path :
        return self.__dbfile_path.joinpath(f"{name}.json")

    def retrieve(self, name : str) -> typing.Any :
        file_path = self.__get_json_file_path(name)
        try :
            assert file_path.exists() and file_path.is_file(), f"Cannot find file at {file_path}"
            some_object = json_read(file_path)
            return some_object
        except Exception as e :
            print(f"Tried to get file {file_path} but hit :\n{e}")
            return None
        
    def get_names(self) :
        name_list = []
        for folder_entry in self.__dbfile_path.iterdir() :
            if folder_entry.is_file() and folder_entry.suffix == ".json" :
                name_list.append(folder_entry.stem)
            else :
                debug_message(f"Found non-database folder entry \"{folder_entry}\"")
        return name_list
    
    def drop(self, name : str) -> bool :
        if self.is_stored(name) :
            file_path = self.__get_json_file_path(name)
            file_path.unlink()
            return True
        return False

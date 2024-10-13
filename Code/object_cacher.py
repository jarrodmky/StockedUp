import typing
from Code.database import JsonDataBase

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

class ObjectCacher :

    def __init__(self, hash_db : JsonDataBase, hash_object_name : str, default_object : typing.Any) :
        self.__hash_db = hash_db
        self.__hash_object_name = hash_object_name
        self.__default_object = default_object
        self.__default_object_type = type(default_object)
        
    def __get_stored_hashes(self) :
        if self.__hash_db.is_stored(self.__hash_object_name) :
            return self.__hash_db.retrieve(self.__hash_object_name)
        else :
            self.__hash_db.store(self.__hash_object_name, {})
            return {}

    def get_stored_hash(self, name : str) -> str :
        source_hashes = self.__get_stored_hashes()
        if name in source_hashes :
            stored_hash = source_hashes[name]
            assert stored_hash != 0, "Stored 0 hashes forbidden, means import never done or invalid!"
            return stored_hash
        else :
            return "0"

    def set_stored_hash(self, name : str, new_hash : str) -> None :
        assert self.get_stored_hash(name) != new_hash, "Setting new hash without checking it?"
        source_hashes = self.__get_stored_hashes()
        if new_hash != 0 :
            source_hashes[name] = new_hash
        else :
            if name in source_hashes :
                del source_hashes[name]
            else :
                logger.info("Zeroing out hash, something destructive or erroneous happened!")
        self.__hash_db.update(self.__hash_object_name, source_hashes)

    def request_object(self, cache_db : JsonDataBase, object_name : str, current_hash : str, generator : typing.Callable) -> typing.Any :
        result_hash = current_hash
        stored_hash = self.get_stored_hash(object_name)
        if stored_hash == result_hash :
            #hash same, no action
            return cache_db.retrieve(object_name, self.__default_object_type)

        requested_object = None
        try :
            requested_object = generator(object_name)
        except Exception as e :
            logger.error(f"Failed to generate object {object_name}! {e}")
            return self.__default_object
        if isinstance(requested_object, self.__default_object_type) :
            self.set_stored_hash(object_name, result_hash)
            cache_db.update(object_name, requested_object)
            return requested_object
        logger.warning(f"Object {object_name} not expected type, returning default")
        return self.__default_object

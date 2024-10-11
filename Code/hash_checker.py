from Code.database import JsonDataBase

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

class HashChecker :

    def __init__(self, hash_db : JsonDataBase, hash_object_name : str) :
        self.__hash_db = hash_db
        self.__hash_object_name = hash_object_name
        
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

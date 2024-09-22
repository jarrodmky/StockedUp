from pathlib import Path as FilePath
import codecs

class pattern_repeater :
	def __init__(self, pattern : str) :
		self.__pattern : str = pattern
		self.__pattern_str : str = ""
	
	def push(self) -> None :
		self.__pattern_str = self.__pattern + self.__pattern_str

	def pop(self) -> None :
		self.__pattern_str = self.__pattern_str[:-len(self.__pattern)]

	def get_pattern(self) -> str :
		return self.__pattern_str
	
class utf8_file :
	def __init__(self, full_path : FilePath, mode : str) :
		self.__file = codecs.open(str(full_path), mode, "utf-8-sig")
	
	def __enter__(self) :
		assert self.is_good()
		return self
		
	def __exit__(self, *_) :
		self.close()
		
	def close(self) -> None :
		assert self.is_good()
		self.__file.close()
		assert self.__file is not None and self.__file.closed

	def is_good(self) -> bool :
		return self.__file is not None and not self.__file.closed

	def write(self, string : str) -> None :
		assert self.is_good()
		self.__file.write(string)

	def read(self) -> str :
		assert self.is_good()
		return self.__file.read()

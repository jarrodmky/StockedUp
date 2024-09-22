from os import linesep as line_separator
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
		
	def __exit__(self, exc_type, exc_val, exc_tb) :
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


class utf8_writer(utf8_file) :
	def __init__(self, full_path : FilePath, indent : str = "\t") :
		assert full_path.exists()

		utf8_file.__init__(self, full_path, "w")
		self.__indenter = pattern_repeater(indent)
		
	def indent(self) -> None :
		assert self.is_good()
		self.__indenter.push()
		
	def dedent(self) -> None :
		assert self.is_good()
		self.__indenter.pop()
		
	def write_newline(self) -> None :
		self.write(line_separator + self.__indenter.get_pattern())
		
	def write_line(self, string : str = "") -> None :
		self.write(self.__indenter.get_pattern() + string + line_separator)


class utf8_reader(utf8_file) :
	def __init__(self, full_path : FilePath) :
		utf8_file.__init__(self, full_path, "r")
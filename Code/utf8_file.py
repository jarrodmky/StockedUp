from os import linesep as line_separator
import codecs

class pattern_repeater :
	def __init__(self, pattern) :
		self.__pattern = pattern
		self.__pattern_str = ""
	
	def push(self) :
		self.__pattern_str = self.__pattern + self.__pattern_str

	def pop(self) :
		self.__pattern_str = self.__pattern_str[:-len(self.__pattern)]

	def get_pattern(self) :
		return self.__pattern_str
	
class utf8_file :
	def __init__(self, full_path, mode) :
		self.__file = codecs.open(full_path, mode, "utf-8-sig")
	
	def __enter__(self) :
		assert self.is_good()
		return self
		
	def __exit__(self, exc_type, exc_val, exc_tb) :
		assert self.is_good()
		self.close()
		assert self.__file and self.__file.closed
		self.__file = None
		
	def close(self) :
		assert self.is_good()
		self.__file.close()

	def is_good(self) :
		return self.__file and not self.__file.closed

	def write(self, string) :
		assert self.is_good()
		self.__file.write(string)

	def read(self) :
		assert self.is_good()
		return self.__file.read()


class utf8_writer(utf8_file) :
	def __init__(self, full_path, indent = "\t") :
		assert full_path.exists()

		utf8_file.__init__(self, full_path, "w")
		self.__indenter = pattern_repeater(indent)
		
	def indent(self) :
		assert self.is_good()
		self.__indenter.push()
		
	def dedent(self) :
		assert self.is_good()
		self.__indenter.pop()
		
	def write_newline(self) :
		self.write(line_separator + self.__indenter.get_pattern())
		
	def write_line(self, string = "") :
		self.write(self.__indenter.get_pattern() + string + line_separator)


class utf8_reader(utf8_file) :
	def __init__(self, full_path) :
		utf8_file.__init__(self, full_path, "r")
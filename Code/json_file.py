# pylint: disable=no-member
# pylint: disable=method-hidden

import json

from utf8_file import utf8_reader
from utf8_file import utf8_writer
from debug import debug_message

class json_internal :

	class MissingMemberException(Exception):
		
		def __init__(self) :
			self.message = "No member in JSON!!"

	class parse_checker :
		def __del__(self) :
			assert self.__read, "Objects not read! " + str(self.__dictionary)

		def __init__(self, object_dictionary) :
			self.__dictionary = object_dictionary
			self.__read = False

		def __getitem__(self, key):
			if key not in self.__dictionary :
				self.__read = True
				raise json_internal.MissingMemberException()
			value = self.__dictionary[key]
			del self.__dictionary[key]
			self.__read = not self.__dictionary 
			return value

		def read_optional(self, key, default):
			if key in self.__dictionary :
				value = self.__dictionary[key]
				del self.__dictionary[key]
				self.__read = not self.__dictionary 
				return value
			else :
				return default





class json_encoder(json.JSONEncoder) :

	serializable_types = set()

	def __init__(self, *args, **kwargs) :
		json.JSONEncoder.__init__(self, indent=2, sort_keys=True)

	def default(self, object_instance) :
		if type(object_instance) in json_encoder.serializable_types :
			return object_instance.encode()
		
		return super(json_encoder, self).default(object_instance)

def json_register_writeable(some_type) :
	assert some_type not in json_encoder.serializable_types, "Tried to reregister a " + str(some_type)
	debug_message(f"Registering {some_type} as serializable")
	json_encoder.serializable_types.add(some_type)

def json_write(file_path, something) :
	with utf8_writer(file_path) as write_file :
		json.dump(something, write_file, cls=json_encoder)


class json_decoder(json.JSONDecoder) :

	deserializable_types = set()

	def __init__(self, *args, **kwargs) :
		json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

	def object_hook(self, object_dictionary) :
		found_constructor = False
		made_object = None
		for type_of in json_decoder.deserializable_types :
			try :
				wrapper = json_internal.parse_checker(object_dictionary.copy())
				made_object = type_of.decode(wrapper)
				assert found_constructor is False, "Ambiguous object construction found: " + str(type_of)
				found_constructor = True
			except json_internal.MissingMemberException :
				pass

		if found_constructor :
			return made_object
		else :
			return object_dictionary

def json_register_readable(some_type) :
	assert some_type not in json_decoder.deserializable_types, "Tried to reregister a " + str(some_type)
	debug_message(f"Registering {some_type} as deserializable")
	json_decoder.deserializable_types.add(some_type)

def json_read(file_path) :
	with utf8_reader(file_path) as read_file :
		read_object = json.load(read_file, cls=json_decoder)
		assert read_object is not None, "No deserializable object found!"
		return read_object

global_debug_enabled = False
import typing

def debug_message(printed_string : str, condition : bool = True) -> None :
	if global_debug_enabled and condition :
		print("[MESSAGE] : " + printed_string)

def debug_assert(condition : bool, printed_string : str = "") -> None :
	if global_debug_enabled and not condition :
		if printed_string == "" :
			printed_string = "CONDITION FAILED!"
		assert condition, "[ASSERT MESSAGE] : " + printed_string

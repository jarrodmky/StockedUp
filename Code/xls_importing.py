import pathlib
import typing
from xml.parsers.expat import ParserCreate

from PyJMy.debug import debug_message
from accounting_objects import Transaction

class XlsFormat :

    column_list = ["Entry Date", "Settlement Date", "Action", "Qty", "Symbol", "Description", "Price", "Comm.", "Net Amount"]

    @staticmethod
    def make_transaction(_ : typing.List[str]) -> typing.Optional[Transaction] :

        return None
    
class XlsTableReader :

    def __init__(self) :
        self.column_list : typing.List[str] = []
        self.column_size : int = 0
        self.row_list : typing.List[typing.List[str]] = []
        self.current_row : typing.List[str] = []
        self.current_char_data : str = ""

    def start_element(self, name) :
        match name :
            case "table" :
                self.row_list = []
            case "tr" :
                self.current_row = []
            case _ :
                pass
        self.current_char_data = ""

    def end_element(self, name) :
        match name :
            case "tr" :
                if len(self.current_row) == self.column_size :
                    self.row_list.append(self.current_row)
            case "th" :
                self.column_list.append(self.current_char_data)
                self.column_size += 1
            case "td" :
                self.current_row.append(self.current_char_data)
            case _ :
                pass

    def read_char_data(self, data) :
        self.current_char_data += data.strip()

def read_transactions_from_xls(input_file : pathlib.Path) -> typing.List[Transaction] :
    assert input_file.suffix == ".xls"
    debug_message(f"Reading in {input_file}")

    reader = XlsTableReader()

    parser = ParserCreate()
    parser.StartElementHandler = lambda name, _ : reader.start_element(name)
    parser.EndElementHandler = lambda name : reader.end_element(name)
    parser.CharacterDataHandler = lambda data : reader.read_char_data(data)

    try:
        parser.ParseFile(open(input_file, "rb"))
    except Exception as e :
        print("Exception while parsing xls file!")
        return []

    if reader.column_list == XlsFormat.column_list :
        transaction_list : typing.List[Transaction] = []
        for data in reader.row_list :
            opt_transaction = XlsFormat.make_transaction(data)
            if opt_transaction is not None :
                transaction = opt_transaction
                transaction_list.append(transaction)
        return transaction_list
    else :
        assert False, f"Format not recognized! File {input_file}\n Types :\n {reader.column_list}"

import requests # type: ignore
import pathlib
import json
import typing

from accounting import Account, Ledger
from debug import debug_assert, debug_message
import math

import tkinter as tk
from tkinter import ttk
from tkinter.filedialog import askdirectory, askopenfilename

from tkintertable.Tables import TableCanvas # type: ignore
from tkintertable.TableModels import TableModel # type: ignore

data_path = pathlib.Path("Data").absolute()
if not data_path.exists() :
    data_path.mkdir()

FloatList = typing.List[float]
IntegerList = typing.List[int]

class Colour :
    red = [255, 0, 0, 255]
    yellow = [255, 255, 0, 255]
    green = [0, 255, 0, 255]
    cyan = [0, 255, 255, 255]
    blue = [0, 0, 255, 255]
    magenta = [255, 0, 255, 255]

colour_array = [Colour.red, Colour.yellow, Colour.green, Colour.cyan, Colour.blue, Colour.magenta]

api_key_path = data_path.joinpath("alpha_vantage_key.txt")

def read_api_key() :
    with open(api_key_path) as key_file :
        line = key_file.readline()
        return line

api_key = read_api_key()

def retrieve_time_series_data(ticker_symbol : str) -> bool :
    ticker_series_file_path = data_path.joinpath("TimeSeriesRaw").joinpath(ticker_symbol + ".txt")
    if not ticker_series_file_path.exists() :
        parameters = {'function':'TIME_SERIES_DAILY','symbol':ticker_symbol, 'apikey':api_key}
        received_data = requests.get(r'https://www.alphavantage.co/query', params=parameters)
        
        print("Retrieving " + ticker_symbol + " data...")

        if received_data.ok :
            with open(ticker_series_file_path,'wb') as text_file :
                text_file.write(received_data.content)

            print("data recieved!")

            return True

        else :
            print("request failed!")


    return False


retrieve_time_series_data("IBM")
retrieve_time_series_data("T")
retrieve_time_series_data("AMD")
retrieve_time_series_data("CSIQ")
retrieve_time_series_data("QCOM")
retrieve_time_series_data("MSFT")
retrieve_time_series_data("GE")
retrieve_time_series_data("AMZN")
retrieve_time_series_data("SPCE")
retrieve_time_series_data("RCI")
retrieve_time_series_data("BRK-B")



def pack_raw_time_series(ticker_symbol : str) -> None :
    raw_series_file_path = data_path.joinpath("TimeSeriesRaw").joinpath(ticker_symbol + ".txt")
    ticker_series_file_path = data_path.joinpath("TimeSeries").joinpath(ticker_symbol + ".ser")
    if not ticker_series_file_path.exists() :
        print("Packing " + ticker_symbol + " data...")

        if raw_series_file_path.exists() :
            with open(raw_series_file_path, 'r') as raw_file :
                print(json.load(raw_file))

            #with open(ticker_series_file_path,'w') as binary_file :
            #    pickle.dump()

            #print("data packed!")


#pack_raw_time_series("IBM")

class AccountDataPlot :

    def __init__(self, account : Account, startTimestamp : float, endTimestamp : float, colour : IntegerList) :
        self.name = account.name
        self.independent = []
        self.dependent = []
        self.colour = colour

        current_value = account.start_value
        self.independent.append(startTimestamp)
        self.dependent.append(current_value)

        for transaction in account.transactions :
            current_value += transaction.delta
            self.independent.append(transaction.timestamp)
            self.dependent.append(current_value)

        debug_assert(round(current_value, 2) == account.end_value, "Mismatched totals... total = " + str(current_value) + ", end_value = " + str(account.end_value))
        self.independent.append(endTimestamp)
        self.dependent.append(account.end_value)
    
def load_and_plot_base_accounts() :
    base_accounts = load_base_accounts()
    data_sets = []

    min_timestamp = math.inf
    max_timestamp = -math.inf
    for account in base_accounts :
        print("Loaded " + account.name)
        start_timestamp = account.transactions[0].timestamp
        end_timestamp = account.transactions[-1].timestamp
        debug_assert(start_timestamp < end_timestamp)
        if(min_timestamp > start_timestamp) :
            min_timestamp = start_timestamp
        if(max_timestamp < end_timestamp) :
            max_timestamp = end_timestamp

    colour_index = 0
    for account in base_accounts :
        plot_colour = colour_array[colour_index % len(colour_array)]
        colour_index += 1

        data_set = AccountDataPlot(account, min_timestamp, max_timestamp, plot_colour)
        data_sets.append(data_set)

    with simple.window("Main") :
        core.add_plot("Plot", height=-1)
    
        for data_set in data_sets :
            core.add_line_series("Plot", data_set.name, data_set.independent, data_set.dependent, color=data_set.colour)
    
    core.start_dearpygui(primary_window="Main")

def ask_directory() -> typing.Optional[pathlib.Path] :
    got_directory = askdirectory(initialdir=data_path, mustexist=True)
    if got_directory != None and got_directory != "" and got_directory != "." and got_directory != ".." :
        debug_message(f"Got directory {got_directory}")
        return pathlib.Path(got_directory)
    return None

class ViewTableCanvas(TableCanvas) :
    
    def __init__(self, parent : tk.Widget) :
        TableCanvas.__init__(self, parent, model=TableModel(), read_only=True, width = 600)
        self.createTableFrame()
        self.column_count = 0

    def update_data(self, data_dict : typing.Dict) -> None :
        self.model.deleteRows()
        self.model.importDict(data_dict)
        self.column_count = len(data_dict.keys())
        self.redraw()

    def set_row_selection(self, selected_rows : typing.List[int]) -> None :
        self.clearSelected()
        for index in selected_rows :
            if 0 > index >= self.table.rows :
                debug_message(f"Invalid row index!")
                return
        
        self.multiplerowlist = selected_rows
        self.multiplecollist = range(0, self.column_count)
        self.redraw()


class LedgerViewer(tk.Tk) :

    def __init__(self, ledger_path : pathlib.Path) : 
        #tk init 
        tk.Tk.__init__(self)
        self.title("Ledger Viewer")

        self.ledger_path = ledger_path
        if not self.ledger_path.exists() :
            self.ledger_path.mkdir()

        self.account_manager = Ledger(self.ledger_path)

        #setup GUI
        debug_message(f"Setting up GUI...")
        self.make_menu()

    def __create_account_viewer(self, account_name : str) -> None :
        AccountViewer(self, account_name, self.account_manager.get_account_table(account_name).to_dict(orient="index"))

    def __create_unused_transaction_viewer(self) :
        AccountViewer(self, "Unaccounted", self.account_manager.get_unaccounted_transaction_table().to_dict(orient="index"))

    def make_menu(self) :
        menubar = tk.Menu(self)
        self["menu"] = menubar

        menubar.add_command(label="Account Creator", command=lambda gui_root=self : AccountCreator(gui_root))

        account_menu = tk.Menu(menubar)
        menubar.add_cascade(menu=account_menu, label="Accounts")
        account_name_list = self.account_manager.get_account_names()
        if len(account_name_list) > 0 :
            for account_name in account_name_list :
                account_menu.add_command(label=account_name, command=lambda name=account_name : self.__create_account_viewer(name))
        else :
            menubar.entryconfig("Accounts", state="disabled")

        menubar.add_command(label="Unaccounted", command=lambda : self.__create_unused_transaction_viewer())

    def refresh_menu(self) :
        self["menu"] = None #destroy current menu?
        self.make_menu()



class LedgerSetup(tk.Tk) :

    def __init__(self) : 
        #tk init 
        tk.Tk.__init__(self)
        self.title("Ledger Setup")
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.option_add('*tearOff', False)

        self.ledger_data_path = data_path.joinpath("DEFAULT_LEDGER_FOLDER")

        self.new_ledger_name = tk.StringVar()
        self.new_ledger_name.set("SomeLedger")

        info_frame = ttk.Frame(self)

        ledger_name_label = ttk.Label(info_frame, text="New ledger name : ")
        ledger_name_entry = tk.Entry(info_frame, textvariable=self.new_ledger_name)

        open_button = tk.Button(info_frame, text="Open...", command=lambda : self.prompt_for_open())
        create_button = tk.Button(info_frame, text="Create", command=lambda : self.create_new(self.new_ledger_name.get()))

        #layout membership
        info_frame.grid()

        ledger_name_label.grid(column=0, row=0, sticky=tk.NSEW)
        ledger_name_entry.grid(column=1, row=0, sticky=tk.NSEW)

        open_button.grid(column=0, row=1, sticky=tk.NSEW)
        create_button.grid(column=1, row=1, sticky=tk.NSEW)

    
    def prompt_for_open(self) :
        got_directory = ask_directory()
        if got_directory != None :
            self.ledger_data_path = got_directory
            self.destroy()

    
    def create_new(self, ledger_name : str) -> None :
        self.ledger_data_path = data_path.joinpath(ledger_name)
        self.destroy()



class AccountCreator :

    def __init__(self, gui_root : LedgerViewer) :
        self.window = tk.Toplevel(gui_root)
        window_frame = ttk.Frame(self.window, padding="3 3 12 12", relief="raised")

        self.new_account_name = tk.StringVar()
        self.new_account_name.set("Enter here")

        account_name_label = tk.Label(window_frame, text="Account Name : ")
        account_name_entry = tk.Entry(window_frame, textvariable=self.new_account_name)

        self.csv_input_list_box = tk.Listbox(window_frame)
        self.csv_list : typing.List[pathlib.Path] = []

        get_csv_file = lambda : askopenfilename(defaultextension=".csv", initialdir=data_path)

        add_file_button = tk.Button(window_frame, text="Add .csv file...", command=lambda : self.add_csv_file(get_csv_file()))

        create_action = lambda root=gui_root : self.create_new_account(root)
        create_button = tk.Button(window_frame, text="Create new account", command=create_action)
        
        #layout membership
        window_frame.grid()

        account_name_label.grid()
        account_name_entry.grid()

        self.csv_input_list_box.grid()

        add_file_button.grid()

        create_button.grid()

        #layout configuration

    def add_csv_file(self, file_path : str) -> pathlib.Path :
        self.csv_input_list_box.insert(tk.END, file_path)
        csv_path = pathlib.Path(file_path)
        self.csv_list.append(csv_path)
        return csv_path

    def create_new_account(self, gui_root : LedgerViewer) -> None :
        gui_root.account_manager.create_account_from_csvs(self.new_account_name.get(), self.csv_list, 0.0)
        gui_root.refresh_menu()
        debug_assert(gui_root.account_manager.account_is_created(self.new_account_name.get()))
        self.csv_input_list_box.delete(0, self.csv_input_list_box.size() - 1)
        self.csv_list.clear()


class AccountViewer :

    def __init__(self, gui_root : tk.Tk, account_name : str, account_table : typing.Dict) :
        self.window = tk.Toplevel(gui_root)
        window_frame = ttk.Frame(self.window, padding="3 3 12 12", relief="raised")
        self.window.grid_columnconfigure(0, weight=1)
        self.window.grid_rowconfigure(0, weight=1)
        self.window.option_add('*tearOff', False)

        self.account_name = account_name
        self.account_table = account_table

        #account data init
        self.current_account_name = tk.StringVar()
        self.current_account_name.set(self.account_name)

        #information
        account_name_label = ttk.Label(window_frame, text="Account Name : ")
        account_name_label_value = ttk.Label(window_frame, textvariable=self.current_account_name)

        self.search_string = tk.StringVar()

        search_string_label = tk.Label(window_frame, text="Search : ")
        search_string_entry = tk.Entry(window_frame, textvariable=self.search_string)

        search_action = lambda x=self.search_string : self.search_and_select_table_rows(x.get())
        search_button = tk.Button(window_frame, text="Search", command=search_action)

        #table
        table_frame = ttk.Frame(self.window, padding="3 3 12 12", relief="raised")

        debug_message(f"Populate table with data for {self.account_name}")

        self.account_data_table = ViewTableCanvas(table_frame)
        self.account_data_table.update_data(self.account_table)
        self.account_data_table.adjustColumnWidths()

        #layout membership
        window_frame.grid()

        account_name_label.grid(column=0, row=0, sticky=tk.NSEW)
        account_name_label_value.grid(column=1, row=0, sticky=tk.NSEW)

        search_string_label.grid(column=0, row=1, sticky=tk.NSEW)
        search_string_entry.grid(column=1, row=1, sticky=tk.NSEW)
        search_button.grid(column=2, row=1, sticky=tk.NSEW)

        table_frame.grid(column=0, row=2, sticky=tk.EW)

        #layout configuration

    def search_and_select_table_rows(self, search_string : str) -> None :
        selected_rows = []
        for index, (_, transaction) in enumerate(self.account_table.items()) :
            if search_string in transaction["Description"] :
                selected_rows.append(index)
        self.account_data_table.set_row_selection(selected_rows)


setup = LedgerSetup()
setup.mainloop()

print(f"Opening ledger at ", setup.ledger_data_path)
if setup.ledger_data_path is not None :
    viewer = LedgerViewer(setup.ledger_data_path)
    viewer.mainloop()

#load_and_plot_base_accounts()

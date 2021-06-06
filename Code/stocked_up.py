import requests
import pathlib
import json
import pickle
import typing

from accounting import Account, AccountManager
from debug import debug_assert, debug_message
import math

import tkinter as tk
from tkinter import ttk

from tkintertable.Tables import TableCanvas
from tkintertable.TableModels import TableModel

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

data_path = pathlib.Path("Data")
api_key_path = data_path.joinpath("alpha_vantage_key.txt")

def read_api_key() :
    with open(api_key_path) as key_file :
        line = key_file.readline()
        return line

api_key = read_api_key()

def retrieve_time_series_data(ticker_symbol : str) :
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



def pack_raw_time_series(ticker_symbol : str) :
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



class ViewTableCanvas(TableCanvas) :

    def __init__(self, parent : tk.Widget) :
        TableCanvas.__init__(self, parent, model=TableModel(), read_only=True, width = 600)
        self.createTableFrame()

    def update_data(self, data_dict : typing.Dict) :
        self.model.deleteRows()
        self.model.importDict(data_dict)
        self.redraw()

    def set_row_selection(self, selected_rows : typing.List[int]) :
        self.clearSelected()
        for index in selected_rows :
            if 0 > index >= self.table.rows :
                debug_message("Invalid row index!")
                return
        
        self.multiplerowlist = sorted(selected_rows)
        self.redraw()


class AccountViewer(tk.Tk) :

    def __init__(self) : 
        #tk init 
        tk.Tk.__init__(self)
        self.title("Account Viewer")
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.option_add('*tearOff', False)

        #account data init
        self.table_headers = ["Date", "Delta", "Balance", "Description"]
        self.table_header_count = len(self.table_headers)

        self.account_manager = AccountManager()

        self.current_account_name = tk.StringVar()
        self.current_account_name.set("<>")

        #setup GUI
        debug_message(f"Setting up GUI...")
        self.make_menu()

        #information
        info_frame = ttk.Frame(self)

        account_name_label = ttk.Label(info_frame, text="Account Name : ")
        account_name_label_value = ttk.Label(info_frame, textvariable=self.current_account_name)

        self.search_string = tk.StringVar()

        search_string_label = tk.Label(info_frame, text="Search : ")
        search_string_entry = tk.Entry(info_frame, textvariable=self.search_string)
        search_button = tk.Button(info_frame, text="Search", command=lambda x=self.search_string : self.search_and_select_table_rows(x.get()))

        #table
        table_frame = ttk.Frame(self, padding="3 3 12 12", relief="raised")

        self.account_data_table = ViewTableCanvas(table_frame)

        #layout membership
        info_frame.grid()

        account_name_label.grid(column=0, row=0, sticky=tk.NSEW)
        account_name_label_value.grid(column=1, row=0, sticky=tk.NSEW)

        search_string_label.grid(column=0, row=1, sticky=tk.NSEW)
        search_string_entry.grid(column=1, row=1, sticky=tk.NSEW)
        search_button.grid(column=2, row=1, sticky=tk.NSEW)

        table_frame.grid(column=0, row=2, sticky=tk.EW)

        #layout configuration

        self.select_and_show_account_table(self.account_manager.get_account_names()[0])
        self.account_data_table.adjustColumnWidths()

    def make_menu(self) :
        menubar = tk.Menu(self)
        self["menu"] = menubar

        menubar.add_command(label="Account Creator", command=lambda gui_root=self : AccountCreator(gui_root))

        account_menu = tk.Menu(menubar)
        menubar.add_cascade(menu=account_menu, label="Accounts")
        for account_name in self.account_manager.get_account_names() :
            account_menu.add_command(label=account_name, command=lambda name=account_name : self.select_and_show_account_table(name))

    def refresh_menu(self) :
        self["menu"] = None #destroy current menu?
        self.make_menu()

    def select_and_show_account_table(self, account_name : str) :
        self.current_account_name.set(account_name)

        debug_message(f"Populate table with data for {self.current_account_name.get()}")
        current_account = self.account_manager.get_account_table(account_name)

        debug_assert(current_account != None, "Could not find account!")
        self.account_data_table.update_data(current_account.row_data)

    def search_and_select_table_rows(self, search_string : str) :
        current_account = self.account_manager.get_account_data(self.current_account_name.get())
        selected_rows = []
        for index, transaction in enumerate(current_account.transactions) :
            if search_string == transaction.description :
                selected_rows.append(index)
        self.account_data_table.set_row_selection(selected_rows)

    def create_derived_account(self, account_name : str) :
        self.account_manager.create_derived_account(account_name)
        self.refresh_menu()




class AccountCreator :

    def __init__(self, gui_root : AccountViewer) :
        self.window = tk.Toplevel(gui_root)
        window_frame = ttk.Frame(self.window, padding="3 3 12 12", relief="raised")

        self.new_account_name = tk.StringVar()
        self.new_account_name.set("Enter here")

        account_name_label = tk.Label(window_frame, text="Account Name : ")
        account_name_entry = tk.Entry(window_frame, textvariable=self.new_account_name)
        create_button = tk.Button(window_frame, text="Create new account", command=lambda x=self.new_account_name : gui_root.create_derived_account(x.get()))
        
        #layout membership
        window_frame.grid()
        account_name_label.grid()
        account_name_entry.grid()
        create_button.grid()

        #layout configuration


viewer = AccountViewer()
viewer.mainloop()

#load_and_plot_base_accounts()

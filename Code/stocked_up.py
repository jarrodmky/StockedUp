import requests
import pathlib
import json
import pickle
import typing

from accounting import Account, Transaction, load_base_accounts
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

class AccountDataTable :

    AccountRowType = typing.Tuple[str, float, float, str]

    @staticmethod
    def row(transaction : Transaction, current_balance : float) -> typing.Dict :
        #assume headers as "Date", "Delta", "Balance", "Description"
        return { "Date" : transaction.date, "Delta" : transaction.delta, "Balance" : round(current_balance, 2), "Description" : transaction.description }

    def __init__(self, account : Account) :
        self.name = account.name
        self.row_data : typing.Dict = {}
        current_balance = account.start_value
        index : int = 0
        for transaction in account.transactions :
            current_balance += transaction.delta
            self.row_data[str(index)] = AccountDataTable.row(transaction, current_balance)
            index += 1

    def row_count(self) -> int :
        return len(self.row_data)
    
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

class AccountViewer(tk.Tk) :

    def __init__(self, accounts : typing.List[Account]) : 
        #tk init 
        tk.Tk.__init__(self)
        self.title("Account Viewer")
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.option_add('*tearOff', False)

        #account data init
        self.table_headers = ["Date", "Delta", "Balance", "Description"]
        self.table_header_count = len(self.table_headers)
        
        self.account_lookup : typing.Mapping[str, AccountDataTable] = {}

        for account in accounts :
            self.account_lookup[account.name] = AccountDataTable(account)

        self.current_account_name = tk.StringVar()
        self.current_account_name.set("<>")

        #setup GUI
        debug_message(f"Setting up GUI...")
        menubar = tk.Menu(self)
        self["menu"] = menubar

        account_menu = tk.Menu(menubar)
        menubar.add_cascade(menu=account_menu, label="Base Accounts")

        for account in accounts :
            account_menu.add_command(label=account.name, command=lambda name=account.name : self.select_and_show_account_table(name))

        
        #information
        info_frame = ttk.Frame(self)

        account_name_label = ttk.Label(info_frame, text="Account Name : ")
        account_name_label_value = ttk.Label(info_frame, textvariable=self.current_account_name)

        #table
        table_frame = ttk.Frame(self, padding="3 3 12 12", relief="raised")

        self.account_data_table = TableCanvas(table_frame, model=TableModel(), read_only=True)
        self.account_data_table.createTableFrame()
        self.account_data_table_model = self.account_data_table.model

        #layout membership
        info_frame.grid()

        account_name_label.grid(column=0, row=0, sticky=(tk.W, tk.E))
        account_name_label_value.grid(column=1, row=0, sticky=(tk.W, tk.E))

        table_frame.grid(column=0, row=1, sticky=(tk.N, tk.W, tk.E, tk.S), columnspan=4)

        #layout configuration

        
        self.select_and_show_account_table(accounts[0].name)

    
    def select_and_show_account_table(self, account_name : str) :
        self.current_account_name.set(account_name)

        debug_message(f"Populate table with data for {self.current_account_name.get()}")
        current_account = self.account_lookup[account_name]
        self.account_data_table_model.deleteRows()
        self.account_data_table_model.importDict(current_account.row_data)
        self.account_data_table.redraw()


base_accounts = load_base_accounts()

viewer = AccountViewer(base_accounts)
viewer.mainloop()

#load_and_plot_base_accounts()

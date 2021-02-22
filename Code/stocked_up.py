import requests
import pathlib
import json
import pickle
import typing

from dearpygui import core, simple
from manage_ledger import load_base_accounts
from accounting import Account
from debug import debug_assert
import math

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

class DataPlot :

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

        data_set = DataPlot(account, min_timestamp, max_timestamp, plot_colour)
        data_sets.append(data_set)

    with simple.window("Main") :
        core.add_plot("Plot", height=-1)
    
        for data_set in data_sets :
            core.add_line_series("Plot", data_set.name, data_set.independent, data_set.dependent, color=data_set.colour)
    
    core.start_dearpygui(primary_window="Main")

def populate_account_table(account : Account) :
    core.add_text(f"Name : {account.name}")
    core.add_table("Account", ["Date", "Delta", "Balance", "Description"], height =-1)
    current_balance = account.start_value
    for transaction in account.transactions :
        current_balance += transaction.delta
        core.add_row("Account", [transaction.date, transaction.delta, round(current_balance, 2), transaction.description])

def change_display_account(sender, data) :
    #populate_account_table(data[sender])
    pass

def load_and_generate_derived_accounts() :
    base_accounts = load_base_accounts()
    account_lookup = {}

    for account in base_accounts :
        account_lookup[account.name] = account

    with simple.window("Main") :
        with simple.menu_bar("MenuBar") :
            with simple.menu("Base Accounts") :
                for account in base_accounts :
                    core.add_menu_item(account.name, callback=change_display_account, callback_data=account_lookup)
                
        populate_account_table(base_accounts[3])
    
    core.start_dearpygui(primary_window="Main")

#load_and_plot_base_accounts()
load_and_generate_derived_accounts()

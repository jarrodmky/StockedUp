import requests
import pathlib
import json
import pickle
import typing

from dearpygui.dearpygui import *
from manage_ledger import load_base_accounts
from accounting import Account
from debug import debug_assert
import math

Pair = typing.List[float]
CoordinateData = typing.List[Pair]

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


def example_callback(sender, data):
    print("Save Clicked")

class DataPlot :

    def __init__(self, name : str, data : CoordinateData) :
        self.name = name
        self.data = data

def display(data_sets : typing.List[DataPlot]) :

    add_text("Hello world")
    add_button("Save", callback=example_callback)
    add_input_text("string")
    add_slider_float("float")
    
    add_plot("Plot", "day", "price")
    
    for data_set in data_sets :
        add_line_series("Plot", data_set.name, data_set.data)
    
    start_dearpygui()



#data1 = []
#for i in range(0, 10) :
#    data1.append([i, i*i])
#
#data2 = []
#for i in range(0, 10) :
#    data2.append([i, i*i*i - 6*i])
#
#display([data1, data2])

def account_to_stream(account : Account, startTimestamp : float, endTimestamp) -> CoordinateData :

    coords = []
    current_value = account.start_value
    coords.append([startTimestamp, current_value])

    for transaction in account.transactions :
        current_value += transaction.delta
        coords.append([transaction.timestamp, current_value])

    debug_assert(round(current_value, 2) == account.end_value, "Mismatched totals... total = " + str(current_value) + ", end_value = " + str(account.end_value))
    coords.append([endTimestamp, account.end_value])

    return coords
    
def load_and_plot_base_accounts() :
    base_accounts = load_base_accounts()
    account_streams = []

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

    for account in base_accounts :
        data_set = DataPlot(account.name, account_to_stream(account, min_timestamp, max_timestamp))
        account_streams.append(data_set)

    display(account_streams)

load_and_plot_base_accounts()

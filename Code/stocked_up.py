import requests
import pathlib

from dearpygui.dearpygui import *

from math import cos, sin

data_path = pathlib.Path("Data")
api_key_path = data_path.joinpath("alpha_vantage_key.txt")

def read_api_key() :
    with open(api_key_path) as key_file :
        line = key_file.readline()
        return line

api_key = read_api_key()

def retrieve_time_series_data(ticker_symbol) :
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



def save_callback(sender, data):
    print("Save Clicked")

add_text("Hello world")
add_button("Save", callback=save_callback)
add_input_text("string")
add_slider_float("float")

add_plot("Plot", "x-axis", "y-axis", height=-1)

data1 = []
for i in range(0, 100):
    data1.append([3.14 * i / 180, cos(3 * 3.14 * i / 180)])

data2 = []
for i in range(0, 100):
    data2.append([3.14 * i / 180, sin(2 * 3.14 * i / 180)])

add_line_series("Plot", "Cos", data1, weight=2, fill=[255, 0, 0, 100])
add_scatter_series("Plot", "Sin", data2)

start_dearpygui()

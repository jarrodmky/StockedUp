import requests
import pathlib
import json
import pickle
import typing

from dearpygui import core, simple
from accounting import Account, Transaction, load_base_accounts
from debug import debug_assert, debug_message
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
    def row(transaction : Transaction, current_balance : float) -> AccountRowType :
        return [transaction.date, transaction.delta, round(current_balance, 2), transaction.description]

    def __init__(self, account : Account) :
        self.name = account.name
        self.row_data : typing.List[AccountRowType] = []
        current_balance = account.start_value
        #self.row_data = map(lambda transaction : AccountDataTable.row(transaction, current_balance += transaction.delta), account.transactions)
        for transaction in account.transactions :
            #assume headers as "Date", "Delta", "Balance", "Description"
            current_balance += transaction.delta
            self.row_data.append(AccountDataTable.row(transaction, current_balance))

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

class AccountViewer :

    def __init__(self, accounts : typing.List[Account]) :           
        self.table_headers = ["Date", "Delta", "Balance", "Description"]
        self.table_header_count = len(self.table_headers)
        
        self.account_lookup : typing.Mapping[str, AccountDataTable] = {}
        self.current_account : AccountDataTable = None

        for account in accounts :
            self.account_lookup[account.name] = AccountDataTable(account)

        with simple.window("Main") :
            with simple.menu_bar("MenuBar") :
                with simple.menu("Base Accounts") :
                    for account in accounts :
                        core.add_menu_item(account.name, callback=AccountViewer.select_account_callback, callback_data=(account.name, self))
            debug_message(f"Creating table with title and columns")
            core.add_text("AccountName")
            core.add_table("AccountData", self.table_headers, height =-1, callback=AccountViewer.click_table_callback, callback_data=self)
            self.select_and_show_account_table(accounts[0].name)

        #core.show_logger()
        #logging_level = 0
        #core.set_log_level(logging_level)

        #core.log("trace message")
        #core.log_debug("debug message")
        #core.log_info("info message")
        #core.log_warning("warning message")
        #core.log_error("error message")

        core.start_dearpygui(primary_window="Main")
    
    def select_and_show_account_table(self, account_name : str) :
        self.current_account = self.account_lookup[account_name]
        debug_message(f"Populate table with data for {self.current_account.name}")
        core.set_value("AccountName", f"Name : {self.current_account.name}")
        core.set_table_data("AccountData", self.current_account.row_data)
    
    def set_row_is_selected(self, row : int, value : bool) :
        for column in range(self.table_header_count) :
            core.set_table_selection("AccountData", row, column, value)

    def update_table_row_selection(self, table_selection : typing.List[typing.List[int]]) :
        row_selection_bin : typing.List[int] = [0]*self.current_account.row_count()
        for [selected_row, _] in table_selection :
            row_selection_bin[selected_row] += 1

        row_deselect_threshold : int = self.table_header_count - 1
        for (row, row_select_count) in enumerate(row_selection_bin) :
            if row_select_count == 0 or row_select_count == self.table_header_count :
                #stable
                pass
            elif row_select_count == 1 :
                #select
                self.set_row_is_selected(row, True)
            elif row_select_count == row_deselect_threshold :
                #unselect
                self.set_row_is_selected(row, False)
            else :
                debug_message(f"Unhandled row select count {row_select_count}!")

    @staticmethod
    def click_table_callback(sender, account_viewer) :
        #select all the columns on a row that's selected
        debug_message("Processing table select")
        new_table_selection = core.get_table_selections("AccountData")
        account_viewer.update_table_row_selection(new_table_selection)

    @staticmethod
    def select_account_callback(sender, data) :
        (account_name, account_viewer) = data
        account_viewer.select_and_show_account_table(account_name)


base_accounts = load_base_accounts()

AccountViewer(base_accounts)
#load_and_plot_base_accounts()

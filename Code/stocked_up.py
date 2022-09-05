import requests # type: ignore
import pathlib
import json
import typing
from re import compile as compile_expression
from re import sub as replace_matched
from pandas import DataFrame

from accounting import Ledger
from dataframetable import DataFrameTable #needed for kv file load
from debug import debug_message

from kivy.app import App
from kivy.lang import Builder
from kivy.metrics import mm
from kivy.properties import StringProperty, ObjectProperty
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.screenmanager import ScreenManager, Screen, WipeTransition
from kivy.uix.textinput import TextInput
from kivy.uix.treeview import TreeViewNode, TreeViewLabel

data_path = pathlib.Path("Data")
if not data_path.exists() :
    data_path.mkdir()

#---------------------------------------------------------------------------------
#---------------------------------------------------------------------------------
#---------------------------------------------------------------------------------

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

#---------------------------------------------------------------------------------
#---------------------------------------------------------------------------------
#---------------------------------------------------------------------------------

Builder.load_file("stocked_up.kv")

class LedgerNameInput(TextInput):

    pattern = compile_expression('[^A-Za-z0-9_]')

    def insert_text(self, substring, from_undo=False) :
        s = replace_matched(LedgerNameInput.pattern, "", substring)
        return super().insert_text(s, from_undo=from_undo)

class LedgerLoader(Screen) :

    root_path = StringProperty(str(data_path))

    def on_load_ledger(self, path, filename) :
        debug_message("[LedgerLoader] on_load_ledger fired")
        if str(data_path.absolute()) != path :
            debug_message(f"[LedgerLoader] Loading Ledger path: {path}")

            ledger_viewer = LedgerViewer()
            ledger_viewer.set_ledger(Ledger(pathlib.Path(path)))
            self.manager.switch_to(ledger_viewer, direction="left")

class LedgerCreator(Screen) :

    def on_create_ledger(self, ledger_name) :
        debug_message("[LedgerCreator] on_create_ledger fired")
        debug_message(f"[LedgerCreator] Create Ledger named: {ledger_name}")
            
        ledger_path = data_path.joinpath(ledger_name)
        if not ledger_path.exists() :
            ledger_path.mkdir()

        ledger_viewer = LedgerViewer()
        ledger_viewer.set_ledger(Ledger(ledger_path))
        self.manager.switch_to(ledger_viewer, direction="left")

class AccountTreeViewEntry(AnchorLayout, TreeViewNode) :

    account_name_entry = ObjectProperty(None)

    def __init__(self, account_name, account_open_callback, **kwargs) :
        super(AccountTreeViewEntry, self).__init__(**kwargs)

        self.account_name = account_name
        self.account_name_entry.text = account_name
        self.no_selection = True
        self.open_callback = account_open_callback

    def open_account_view(self) :
        self.open_callback(self.account_name)

class LedgerViewer(Screen) :

    tree_view_widget = ObjectProperty(None)

    def set_ledger(self, ledger : Ledger) -> None :
        debug_message("[LedgerViewer] set_ledger called")

        self.ledger = ledger

        self.tree_view_widget.bind(minimum_height = self.tree_view_widget.setter("height"))

        base_node = self.__add_category_node("Base Accounts")
        derived_node = self.__add_category_node("Derived Accounts")

        account_name_list = self.ledger.get_account_names()
        if len(account_name_list) > 0 :
            for account_name in [name for name in account_name_list if not self.ledger.get_account_is_derived(name)] :
                self.__add_account_node(account_name, base_node)
            for account_name in [name for name in account_name_list if self.ledger.get_account_is_derived(name)] :
                self.__add_account_node(account_name, derived_node)
        else :
            self.tree_view_widget.disabled = True

    def __add_category_node(self, name, parent=None) :
        return self.tree_view_widget.add_node(TreeViewLabel(text=name, no_selection=True, is_open=True), parent)

    def __add_account_node(self, account_name, parent) :
        return self.tree_view_widget.add_node(AccountTreeViewEntry(account_name, self.__view_account_transactions), parent)

    def __view_account_transactions(self, account_name : str) -> None :
        account_data = self.ledger.get_account_table(account_name)

        new_screen = AccountViewer(account_name, account_data, [0.1, 0.70, 0.08, 0.12])
        self.manager.switch_to(new_screen, direction="left")

    def view_unused_transactions(self) :
        account_data = self.ledger.get_unaccounted_transaction_table()

        new_screen = AccountViewer("Unaccounted", account_data, [0.05, 0.1, 0.65, 0.08, 0.12])
        self.manager.switch_to(new_screen, direction="left")

class AccountViewer(Screen) :

    account_name_label = ObjectProperty(None)
    account_data_table = ObjectProperty(None)

    def __init__(self, account_name : str, account_data : DataFrame, column_relative_sizes : typing.List[float], **kwargs : typing.ParamSpecKwargs) -> None :
        super(Screen, self).__init__(**kwargs)

        self.account_name_label.text = account_name
        self.account_data_table.set_data_frame(account_data, column_relative_sizes)

    def filter_by_description(self, match_string : str) -> None :
        if match_string == "" :
            self.account_data_table.filter_by(lambda df : df)
        else :
            self.account_data_table.filter_by(lambda df : df[df['Description'].str.contains(match_string, regex=False)])

class CustomScreenManager(ScreenManager) :

    def simple_switch_to(self, screen_name : str) -> typing.Any :
        debug_message(f"[CustomScreenManager] simple_switch_to {screen_name}")
        if self.has_screen(screen_name) :
            screen = self.get_screen(screen_name)
            return super().switch_to(screen, direction="left")
        else :
            debug_message(f"Screen {screen_name} does not exist!")
            return None

    def destructive_switch_to(self, screen_name : str, destroy_screen : Screen) -> None :
        debug_message(f"[CustomScreenManager] destructive_switch_to {screen_name}")
        self.simple_switch_to(screen_name)
        self.remove_widget(destroy_screen)

class StockedUpApp(App) :

    def build(self) :
        debug_message("[StockedUpApp] build fired")

        self.scroll_bar_colour = [0.2, 0.7, 0.9, .5]
        self.scroll_bar_inactive_colour = [0.2, 0.7, 0.9, .5]
        self.scroll_bar_width = mm(4)
        self.fixed_button_height = mm(8)
        
        self.account_viewer_fixed_size = (3 * self.fixed_button_height + mm(6))
        self.account_viewer_fixed_row_height = 12

        return CustomScreenManager(transition=WipeTransition())

if __name__ == '__main__' :
    StockedUpApp().run()

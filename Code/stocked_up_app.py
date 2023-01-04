import pathlib
import typing
from re import compile as compile_expression
from re import sub as replace_matched
from pandas import DataFrame

from kivy import require as version_require
from kivy.app import App
from kivy.config import Config
from kivy.core.window import Window
from kivy.lang import Builder
from kivy.metrics import mm
from kivy.properties import StringProperty, ObjectProperty
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.screenmanager import ScreenManager, Screen, WipeTransition
from kivy.uix.textinput import TextInput
from kivy.uix.treeview import TreeViewNode, TreeViewLabel

from PyJMy.json_file import json_register_readable, json_read
from PyJMy.debug import debug_message

from accounting import Ledger
from dataframetable import DataFrameTable #needed for kv file load

class AccountImport :

    def __init__(self) :
        self.folder : str = "<INVALID FOLDER>"
        self.opening_balance : float = 0.0

    @staticmethod
    def decode(reader) :
        new_account_import = AccountImport()
        new_account_import.folder = reader["folder"]
        new_account_import.opening_balance = reader.read_optional("opening balance", 0.0)
        return new_account_import

json_register_readable(AccountImport)

class LedgerImport :

    def __init__(self) :
        self.name : str = "<INVALID LEDGER>"
        self.raw_accounts : typing.List[AccountImport] = []

    @staticmethod
    def decode(reader) :
        new_ledger_import = LedgerImport()
        new_ledger_import.name = reader["name"]
        new_ledger_import.raw_accounts = reader["raw accounts"]
        return new_ledger_import

json_register_readable(LedgerImport)

def kivy_initialize() :
    version_require('2.0.0')
    Config.set('input', 'mouse', 'mouse,multitouch_on_demand')
    Builder.load_file("stocked_up.kv")

class LedgerNameInput(TextInput):

    pattern = compile_expression('[^A-Za-z0-9_]')

    def insert_text(self, substring, from_undo=False) :
        s = replace_matched(LedgerNameInput.pattern, "", substring)
        return super().insert_text(s, from_undo=from_undo)

class LedgerLoader(Screen) :

    root_path = StringProperty(None)

    def on_load_ledger(self, path, _) :
        debug_message("[LedgerLoader] on_load_ledger fired")

        if str(self.manager.data_root_directory.absolute()) != path :
            debug_message(f"[LedgerLoader] Loading Ledger path: {path}")

            ledger_viewer = LedgerViewer()
            ledger_viewer.set_ledger(Ledger(pathlib.Path(path)))
            self.manager.switch_to(ledger_viewer, direction="left")

class LedgerCreator(Screen) :

    def on_create_ledger(self, ledger_name) :
        debug_message("[LedgerCreator] on_create_ledger fired")
        debug_message(f"[LedgerCreator] Create Ledger named: {ledger_name}")
            
        ledger_path = self.manager.data_root_directory.joinpath(ledger_name)
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

        base_node = self.__add_category_node("Base Accounts", True)
        derived_node = self.__add_category_node("Derived Accounts", True)

        account_name_list = self.ledger.get_account_names()
        if len(account_name_list) > 0 :
            #show base accounts as list
            for account_name in self.ledger.get_base_account_names() :
                self.__add_account_node(account_name, base_node)

            #show derived accounts as tree
            root_category = self.ledger.category_tree.get_root()
            self.__add_nodes_recursive(root_category, derived_node)
        else :
            self.tree_view_widget.disabled = True

    def __add_nodes_recursive(self, parent_name, parent_node) :
        children = self.ledger.category_tree.get_children(parent_name)
        assert (len(children) > 0) != (self.ledger.account_is_created(parent_name)), "Nodes are either categories (branches) or accounts (leaves)"
        for child_name in children :
            if self.ledger.account_is_created(child_name) :
                self.__add_account_node(child_name, parent_node)
            else :
                category_node = self.__add_category_node(child_name, False, parent_node)
                self.__add_nodes_recursive(child_name, category_node)


    def __add_category_node(self, name, is_open, parent=None) :
        return self.tree_view_widget.add_node(TreeViewLabel(text=name, no_selection=True, is_open=is_open), parent)

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

class StockedUpAppManager(ScreenManager) :

    def simple_switch_to(self, screen_name : str) -> typing.Any :
        debug_message(f"[StockedUpAppManager] simple_switch_to {screen_name}")
        if self.has_screen(screen_name) :
            screen = self.get_screen(screen_name)
            return super().switch_to(screen, direction="left")
        else :
            debug_message(f"Screen {screen_name} does not exist!")
            return None

    def destructive_switch_to(self, screen_name : str, destroy_screen : Screen) -> None :
        debug_message(f"[StockedUpAppManager] destructive_switch_to {screen_name}")
        self.simple_switch_to(screen_name)
        self.remove_widget(destroy_screen)

    def import_ledgers(self) :
        ledger_configuration_path = self.data_root_directory.joinpath("LedgerConfiguration.json")
        ledgers_import_list : typing.List[LedgerImport] = json_read(ledger_configuration_path)["ledgers"]

        for ledger_import in ledgers_import_list :
            ledger_data_path = self.data_root_directory.joinpath(ledger_import.name)
            if not ledger_data_path.exists() :
                debug_message(f"Creating ledger folder {ledger_data_path}")
                ledger_data_path.mkdir()
            ledger = Ledger(ledger_data_path)
            
            for account_import in ledger_import.raw_accounts :
                input_folder_path = self.data_root_directory.joinpath(account_import.folder)
                if not input_folder_path.exists() :
                    raise FileNotFoundError(f"Could not find expected filepath {input_folder_path}")
                input_filepaths = []
                for file_path in input_folder_path.iterdir() :
                    if file_path.is_file() and file_path.suffix == ".csv" :
                        input_filepaths.append(file_path)

                account_name = input_folder_path.stem
                if ledger.account_is_created(account_name) :
                    ledger.delete_account(account_name)
                ledger.create_account_from_csvs(account_name, input_filepaths, account_import.opening_balance)

            ledger.clear()
            ledger.derive_and_balance_accounts()
            ledger.save()
            
        default_ledger_path = self.data_root_directory.joinpath(json_read(ledger_configuration_path)["default ledger"])
        ledger_viewer = LedgerViewer()
        ledger_viewer.set_ledger(Ledger(default_ledger_path))
        self.switch_to(ledger_viewer, direction="left")


class StockedUpApp(App) :

    def __init__(self, data_directory : pathlib.Path, **kwargs : typing.ParamSpecKwargs) :
        super(StockedUpApp, self).__init__(**kwargs)

        Window.size = (1600, 900)
        Window.left = (1920 - 1600) / 2
        Window.top = (1080 - 900) / 2

        if not data_directory.exists() :
            data_directory.mkdir()
        assert data_directory.is_dir(), "Data directory not directory?"
        
        self.data_root_directory = data_directory

    def build(self) :
        debug_message("[StockedUpApp] build fired")

        self.scroll_bar_colour = [0.2, 0.7, 0.9, .5]
        self.scroll_bar_inactive_colour = [0.2, 0.7, 0.9, .5]
        self.scroll_bar_width = mm(4)
        self.fixed_button_height = mm(8)
        
        self.account_viewer_fixed_size = (3 * self.fixed_button_height + mm(6))
        self.account_viewer_fixed_row_height = 12

        screen_manager = StockedUpAppManager(transition=WipeTransition())
        screen_manager.data_root_directory = self.data_root_directory

        return screen_manager

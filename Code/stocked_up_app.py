import pathlib
import typing
import datetime
from re import compile as compile_expression
from re import sub as replace_matched
from pandas import DataFrame
from pathlib import Path
from numpy import Inf

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
from kivy.uix.treeview import TreeViewNode, TreeViewLabel, TreeView

import matplotlib.pyplot as plt

from PyJMy.json_file import json_read
from PyJMy.debug import debug_message

from accounting import Ledger, open_ledger
from dataframetable import DataFrameTable #needed for kv file load

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

            self.manager.load_ledger(pathlib.Path(path))

class LedgerCreator(Screen) :

    def on_create_ledger(self, ledger_name) :
        debug_message("[LedgerCreator] on_create_ledger fired")
        debug_message(f"[LedgerCreator] Create Ledger named: {ledger_name}")
            
        ledger_path = self.manager.data_root_directory.joinpath(ledger_name)
        if not ledger_path.exists() :
            ledger_path.mkdir()

        self.manager.load_ledger(ledger_path)

class LedgerAccountTreeViewNode(AnchorLayout, TreeViewNode) :

    account_name_entry = ObjectProperty(None)

    def __init__(self, account_name, account_open_callback, **kwargs) :
        super(LedgerAccountTreeViewNode, self).__init__(**kwargs)

        self.account_name = account_name
        self.account_name_entry.text = account_name
        self.open_callback = account_open_callback

    def open(self) :
        self.open_callback(self.account_name)

class LedgerViewer(Screen) :

    tree_view_widget = ObjectProperty(None)

    def set_ledger(self, ledger : Ledger) -> None :
        debug_message("[LedgerViewer] set_ledger called")

        self.ledger = ledger

        self.tree_view_widget.bind(minimum_height = self.tree_view_widget.setter("height"))

        base_node = self.__add_internal_node("Base Accounts", True)
        derived_node = self.__add_internal_node("Derived Accounts", True)

        account_name_list = self.ledger.get_account_names()
        if len(account_name_list) > 0 :
            #show base accounts as list
            for account_name in self.ledger.get_base_account_names() :
                self.__add_external_node(account_name, base_node)

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
                self.__add_external_node(child_name, parent_node)
            else :
                category_node = self.__add_internal_node(child_name, False, parent_node)
                self.__add_nodes_recursive(child_name, category_node)


    def __add_internal_node(self, name, is_open, parent=None) :
        return self.tree_view_widget.add_node(TreeViewLabel(text=name, no_selection=True, is_open=is_open), parent)

    def __add_external_node(self, name, parent) :
        return self.tree_view_widget.add_node(LedgerAccountTreeViewNode(name, self.__view_account_transactions), parent)

    def __view_account_transactions(self, account_name : str) -> None :
        account_data = self.ledger.get_account_table(account_name)

        new_screen = AccountViewer(account_name, account_data, [0.1, 0.70, 0.08, 0.12])
        self.manager.push_overlay(new_screen)

    def view_unused_transactions(self) :
        account_data = self.ledger.get_unaccounted_transaction_table()

        new_screen = AccountViewer("Unaccounted", account_data, [0.05, 0.1, 0.65, 0.08, 0.12])
        self.manager.push_overlay(new_screen)

    def show_data_visualizer(self) :

        new_screen = DataPlotter()
        new_screen.set_ledger(self.ledger)
        self.manager.push_overlay(new_screen)

class AnalyzeLedgerTreeIncludeNode(AnchorLayout, TreeViewNode) :
    name_entry = ObjectProperty(None)
    toggle_inclusion = ObjectProperty(None)

    def __init__(self, name, callback, **kwargs) :
        super(AnalyzeLedgerTreeIncludeNode, self).__init__(**kwargs)

        self.name = name
        self.name_entry.text = name
        self.toggle_inclusion.active = True
        self.toggle_callback = callback

    def toggle(self) :
        self.toggle_callback(self)

class TimeValue :
    def __init__(self, time : float, value : float) :
        self.time : float = time
        self.value : float = value
ValueChangeList = typing.List[TimeValue]
TransactionGroupDict = typing.Dict[str, ValueChangeList]

def collect_subtree_timeseries(ledger : Ledger, leaf_accounts : typing.List[str], start_time_point : float, end_time_point : float) -> ValueChangeList :
    start_value : float = 0.0
    value_list : ValueChangeList = []
    for account_name in leaf_accounts :
        account = ledger.get_account_data(account_name)
        start_value -= account.start_value
        for transaction in account.transactions :
            value_list.append(TimeValue(transaction.timestamp, -transaction.delta))
    
    value_list = sorted(value_list, key=lambda tv : tv.time)

    current_value = start_value
    subtree_timeseries : ValueChangeList = [TimeValue(start_time_point, current_value)]
    for time_value in value_list :
        current_value += time_value.value
        subtree_timeseries.append(TimeValue(time_value.time, current_value))
    subtree_timeseries.append(TimeValue(end_time_point, current_value))

    return subtree_timeseries

def get_visible_frontier_nodes(tree_view) :
    debug_message(f"Finding node frontier:")
    for node in tree_view.iterate_visible_nodes_df() :
        if not node.is_open or node.is_leaf :
            debug_message(f"Account {node.name_entry.text} is on frontier")
            yield node


def get_selected_account_sets(ledger : Ledger, tree_view : TreeView, start_time_point : float, end_time_point : float) -> TransactionGroupDict :
            
    time_filter = lambda t : t >= start_time_point and t <= end_time_point

    transaction_groups : TransactionGroupDict = {}
    for node in get_visible_frontier_nodes(tree_view) :
        if node.toggle_inclusion.active :
            leaf_accounts = [n.name_entry.text for n in tree_view.iterate_all_nodes_df(node) if n.is_leaf]
            subtree_timeseries = collect_subtree_timeseries(ledger, leaf_accounts, start_time_point, end_time_point)
            transaction_groups[node.name_entry.text] = [v for v in subtree_timeseries if time_filter(v.time)]

    return transaction_groups

def save_plot_under_filename(file_name : str) -> None :

    file_path = Path.cwd().joinpath(f"{file_name}.png")
    if file_path.exists():
        parent = Path.cwd()
        i = 0
        while file_path.exists() :
            i += 1
            file_path = parent.joinpath(f"{file_name}_{i}.png")
        plt.savefig(f"{file_name}_{i}")
    else :
        plt.savefig(file_name)

def absolute_series_expenses(transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :

    fig, ax = plt.subplots(figsize=(15, 6.5), layout='constrained')
    min_v = Inf
    max_v = -Inf
    for name, series_data in transaction_groups.items() :
        t = [p.time for p in series_data]
        v = [p.value for p in series_data]
        min_v = min(min(v), min_v)
        max_v = max(max(v), max_v)
        ax.step(t, v, where="post", label=name)

    ax.set_xlabel("time")
    ax.set_xlim(from_timestamp, to_timestamp)
    ax.set_ylabel("value")
    ax.set_ylim(min_v, max_v)
    ax.set_title("Absolute Series Expenses")
    ax.legend()

    save_plot_under_filename( f"AbsoluteSeries_{int(from_timestamp)}_{int(to_timestamp)}")

def normalized_series_expenses(transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :
    pass

def absolute_total_expenses(transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :

    fig, ax = plt.subplots(figsize=(15, 6.5), layout='constrained')
    totals = {}
    for name, series_data in transaction_groups.items() :
        totals[name] = sum([p.value for p in series_data])

    min_v = min(totals.values())
    max_v = max(totals.values())
        
    ax.bar(totals.keys(), totals.values(), color='blue', alpha=0.7)
    ax.set_xlabel("Account")
    ax.set_ylabel("Amount")
    ax.set_ylim(min_v, max_v)
    ax.set_title("Absolute Total Expenses")

    save_plot_under_filename( f"AbsoluteTotal_{int(from_timestamp)}_{int(to_timestamp)}")

def normalized_total_expenses(transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :
    pass

class DataPlotter(Screen) :

    from_date_textbox = ObjectProperty(None)
    to_date_textbox = ObjectProperty(None)

    absolute_scale_radio = ObjectProperty(None)
    normalized_scale_radio = ObjectProperty(None)

    series_scale_radio = ObjectProperty(None)
    total_scale_radio = ObjectProperty(None)

    tree_view_widget = ObjectProperty(None)

    def __init__(self, **kwargs : typing.ParamSpecKwargs) -> None :
        super(Screen, self).__init__(**kwargs)

    def set_ledger(self, ledger : Ledger) -> None :
        debug_message("[LedgerViewer] set_ledger called")

        self.ledger = ledger

        self.tree_view_widget.bind(minimum_height = self.tree_view_widget.setter("height"))

        account_name_list = self.ledger.get_account_names()
        if len(account_name_list) > 0 :
            root_node = self.__add_internal_node("External Accounts", True)
            root_category = self.ledger.category_tree.get_root()
            self.__add_nodes_recursive(root_category, root_node)
        else :
            self.tree_view_widget.disabled = True

    def __add_nodes_recursive(self, parent_name, parent_node) :
        children = self.ledger.category_tree.get_children(parent_name)
        assert (len(children) > 0) != (self.ledger.account_is_created(parent_name)), "Nodes are either categories (branches) or accounts (leaves)"
        for child_name in children :
            if self.ledger.account_is_created(child_name) :
                self.__add_external_node(child_name, parent_node)
            else :
                category_node = self.__add_internal_node(child_name, False, parent_node)
                self.__add_nodes_recursive(child_name, category_node)

    def __add_internal_node(self, name, is_open, parent=None) :
        return self.tree_view_widget.add_node(AnalyzeLedgerTreeIncludeNode(name, self.__toggle_subtree, is_open=is_open), parent)

    def __add_external_node(self, name, parent) :
        return self.tree_view_widget.add_node(AnalyzeLedgerTreeIncludeNode(name, self.__toggle_subtree), parent)

    def make_plot(self) :

        assert self.absolute_scale_radio.active != self.normalized_scale_radio.active
        is_absolute = self.absolute_scale_radio.active

        assert self.series_scale_radio.active != self.total_scale_radio.active
        is_series = self.series_scale_radio.active

        from_time_point = datetime.datetime.strptime(self.from_date_textbox.text, "%Y-%b-%d").timestamp()
        to_time_point = datetime.datetime.strptime(self.to_date_textbox.text, "%Y-%b-%d").timestamp()

        transaction_groups = get_selected_account_sets(self.ledger, self.tree_view_widget, from_time_point, to_time_point)

        if is_series :
            if is_absolute :
                absolute_series_expenses(transaction_groups, from_time_point, to_time_point)
            else :
                normalized_series_expenses(transaction_groups, from_time_point, to_time_point)
        else :
            if is_absolute :
                absolute_total_expenses(transaction_groups, from_time_point, to_time_point)
            else :
                normalized_total_expenses(transaction_groups, from_time_point, to_time_point)

    def __toggle_to_root(self, node) :

        is_active = node.toggle_inclusion.active
        node = node.parent_node
        while node is not None or node is not self.tree_view_widget.root :
            node.toggle_inclusion.active = is_active
            node = node.parent_node

    def __toggle_subtree(self, node) :

        is_active = node.toggle_inclusion.active
        for node in self.tree_view_widget.iterate_all_nodes_bf(node) :
            node.toggle_inclusion.active = is_active
            
        

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

    def __init__(self, **kwargs : typing.ParamSpecKwargs) :
        super(ScreenManager, self).__init__(**kwargs)

        self.__overlay_stack : typing.List[Screen] = []

    def swap_screen(self, screen_name : str) -> typing.Any :
        debug_message(f"[StockedUpAppManager] swap_screen {screen_name}")
        if len(self.__overlay_stack) > 1 :
            debug_message(f"Failed, currently has overlays! {[s.name for s in self.__overlay_stack]}")
            return None
        elif self.has_screen(screen_name) :
            if len(self.__overlay_stack) > 0 :
                debug_message(f"Previous = {self.__overlay_stack[-1].name}")
                assert self.current == self.__overlay_stack[-1].name
                self.__overlay_stack.pop()
            else :
                debug_message(f"First screen pushed, so no previous, current = {self.current}")
            screen = self.get_screen(screen_name)
            return self.push_overlay(screen)
        else :
            debug_message(f"Failed, screen {screen_name} does not exist!")
            return None

    def push_overlay(self, screen : Screen) -> typing.Any :
        debug_message(f"[StockedUpAppManager] push_overlay {self.current} -> {screen.name}")
        self.__overlay_stack.append(screen)
        return super().switch_to(screen, direction="left")

    def pop_overlay(self) -> typing.Any :
        assert len(self.__overlay_stack) > 1, "No overlays on stack!"
        assert self.current == self.__overlay_stack[-1].name
        debug_message(f"[StockedUpAppManager] pop_overlay {self.current} -> {self.__overlay_stack[-2].name}")
        next_screen = super().switch_to(self.__overlay_stack[-2], direction="left")
        self.remove_widget(self.__overlay_stack.pop())
        return next_screen

    def import_ledgers(self) :

        for ledger_import in self.ledger_configuration.ledgers :
            ledger_data_path = self.data_root_directory.joinpath(ledger_import.name)
            if not ledger_data_path.exists() :
                debug_message(f"Creating ledger folder {ledger_data_path}")
                ledger_data_path.mkdir()
            Ledger(ledger_data_path, ledger_import)
            
        self.load_default_ledger()

    def load_ledger(self, ledger_path : pathlib.Path) -> None :
        ledger = open_ledger(ledger_path)
        if ledger is not None :
            ledger_viewer = LedgerViewer()
            ledger_viewer.set_ledger(ledger)
            self.push_overlay(ledger_viewer)

    def load_default_ledger(self) :

        default_ledger_path = self.data_root_directory.joinpath(self.ledger_configuration.default_ledger)
        self.load_ledger(default_ledger_path)

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
        screen_manager.ledger_configuration = json_read(self.data_root_directory.joinpath("LedgerConfiguration.json"))

        screen_manager.swap_screen("LedgerSetup")
        return screen_manager

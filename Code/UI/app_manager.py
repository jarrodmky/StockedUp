import pathlib
import typing
import datetime
from re import compile as compile_expression
from re import sub as replace_matched
from polars import DataFrame, concat
from pathlib import Path
from numpy import Inf

from kivy.properties import StringProperty, ObjectProperty
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.screenmanager import ScreenManager, Screen, WipeTransition
from kivy.uix.textinput import TextInput
from kivy.uix.treeview import TreeViewNode, TreeViewLabel
from kivy.logger import Logger
from kivy.lang.builder import Builder

import matplotlib.pyplot as plot_system
from Code.UI.nametreeviewer import NameTreeViewer
from Code.accounting import Ledger, LedgerImport
from Code.ledger_database import make_account_data_table

Builder.load_file("Code/UI/kv/stockedup.kv")

class LedgerNameInput(TextInput):

    pattern = compile_expression('[^A-Za-z0-9_]')

    def insert_text(self, substring, from_undo=False) :
        s = replace_matched(LedgerNameInput.pattern, "", substring)
        return super(LedgerNameInput, self).insert_text(s, from_undo=from_undo)

class LedgerLoader(Screen) :

    root_path = StringProperty(None)

    def on_load_ledger(self, path, _) :
        Logger.info("[LedgerLoader] on_load_ledger fired")

        if str(self.manager.data_root_directory.absolute()) != path :
            Logger.info(f"[LedgerLoader] Loading Ledger path: {path}")

            self.manager.load_ledger(pathlib.Path(path))

class LedgerCreator(Screen) :

    def __init__(self, **kwargs):
        super(LedgerCreator, self).__init__(**kwargs)

    def on_create_ledger(self, ledger_name) :
        Logger.info("[LedgerCreator] on_create_ledger fired")
        Logger.info(f"[LedgerCreator] Create Ledger named: {ledger_name}")
            
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
        Logger.info("[LedgerViewer] set_ledger called")

        self.ledger = ledger
        internal_node_cb = lambda name, is_active : TreeViewLabel(text=name, no_selection=True, is_open=is_active)
        external_node_cb = lambda name : LedgerAccountTreeViewNode(name, self.__view_account_transactions)
            
        self.tree_view_widget.init_tree_viewer(internal_node_cb, external_node_cb)
        self.tree_view_widget.add_list("Base Accounts", self.ledger.get_source_account_names())
        self.tree_view_widget.add_tree("Derived Accounts", self.ledger.category_tree)

    def __view_account_transactions(self, account_name : str) -> None :
        try :
            account_data = make_account_data_table(self.ledger.get_account(account_name))
            new_screen = AccountViewer(account_name, account_data, [0.1, 0.70, 0.08, 0.12])
            self.manager.push_overlay(new_screen)
        except Exception as e :
            Logger.error(f"Tried to view account, but hit :\n{e}")

    def view_unused_transactions(self) :
        account_data = self.ledger.__database.get_unaccounted_transaction_table()

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

TransactionGroupDict = typing.Dict[str, DataFrame]

def get_full_subtree_timeseries(ledger : Ledger, leaf_accounts : typing.List[str]) -> DataFrame :
    
    total_start_value : float = 0.0
    df_list = []
    for account_name in leaf_accounts :
        account = ledger.get_account(account_name)
        total_start_value -= account.start_value
        df_list.append(DataFrame({
            "timestamp" : account.transactions["timestamp"],
            "delta" : -account.transactions["delta"]
        }))
    
    subtree_timeseries = concat(df_list).sort("timestamp", maintain_order=True)
    subtree_timeseries = subtree_timeseries.groupby("timestamp").sum()
    subtree_timeseries["delta"] = total_start_value + subtree_timeseries["delta"].cumsum()

    first_row = DataFrame({"timestamp": [0.0], "balance": [total_start_value]})
    balances = DataFrame({"timestamp": subtree_timeseries["timestamp"], "balance": total_start_value + subtree_timeseries["delta"].cumsum()})
    return concat([first_row, balances])

def query_frame(df : DataFrame, condition_string : str) -> DataFrame :
    df.sql(f"SELECT * FROM self WHERE {condition_string}")
    return df


def collect_subtree_timeseries(ledger : Ledger, leaf_accounts : typing.List[str], start_time_point : float, end_time_point : float) -> DataFrame :
    
    subtree_timeseries = get_full_subtree_timeseries(ledger, leaf_accounts)

    existing_start_time_point = query_frame(subtree_timeseries, f"timestamp == {start_time_point}")
    if len(existing_start_time_point) == 0 :
        earlier_timepoints = query_frame(subtree_timeseries, f"timestamp < {start_time_point}")
        select_first_row = DataFrame({"timestamp": [start_time_point], "balance": earlier_timepoints.tail(1)["balance"]}) #assume one row
        subtree_timeseries = concat([select_first_row, subtree_timeseries])

    existing_end_time_point = query_frame(subtree_timeseries, f"timestamp == {end_time_point}")
    if len(existing_end_time_point) == 0 :
        earlier_timepoints = query_frame(subtree_timeseries, f"timestamp < {end_time_point}")
        select_last_row = DataFrame({"timestamp": [end_time_point], "balance": earlier_timepoints.tail(1)["balance"]}) #assume one row
        subtree_timeseries = concat([subtree_timeseries, select_last_row])
        
    return query_frame(query_frame(subtree_timeseries, f"timestamp >= {start_time_point}"), f"timestamp <= {end_time_point}")

def get_selected_account_sets(ledger : Ledger, tree_view : NameTreeViewer, start_time_point : float, end_time_point : float) -> TransactionGroupDict :
    transaction_groups : TransactionGroupDict = {}
    for node in tree_view.get_visible_frontier_nodes() :
        if node.toggle_inclusion.active :
            leaf_accounts = [n.name_entry.text for n in tree_view.get_all_subtree_nodes(node) if n.is_leaf]
            transaction_groups[node.name_entry.text] = collect_subtree_timeseries(ledger, leaf_accounts, start_time_point, end_time_point)

    return transaction_groups

class DataPlotter(Screen) :

    from_date_textbox = ObjectProperty(None)
    to_date_textbox = ObjectProperty(None)

    absolute_scale_radio = ObjectProperty(None)
    normalized_scale_radio = ObjectProperty(None)

    series_scale_radio = ObjectProperty(None)
    total_scale_radio = ObjectProperty(None)

    tree_view_widget = ObjectProperty(None)
    figure_container = ObjectProperty(None)

    def __init__(self, **kwargs : typing.ParamSpecKwargs) -> None :
        super(DataPlotter, self).__init__(**kwargs)

    def set_ledger(self, ledger : Ledger) -> None :
        Logger.info("[LedgerViewer] set_ledger called")

        self.ledger = ledger

        internal_node_cb = lambda name, is_active : AnalyzeLedgerTreeIncludeNode(name, self.__toggle_subtree, is_open=is_active)
        external_node_cb = lambda name : AnalyzeLedgerTreeIncludeNode(name, self.__toggle_subtree)

        self.tree_view_widget.init_tree_viewer(internal_node_cb, external_node_cb)
        self.tree_view_widget.add_tree("External Accounts", self.ledger.category_tree)

    def absolute_series_expenses(self, transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :

        min_v = Inf
        max_v = -Inf
        for name, series_data in transaction_groups.items() :
            t = series_data["timestamp"]
            v = series_data["balance"]
            min_v = min(min(v), min_v)
            max_v = max(max(v), max_v)
            self.plotted_axis.step(t, v, where="post", label=name)

        self.plotted_axis.set_xlabel("time")
        self.plotted_axis.set_xlim(from_timestamp, to_timestamp)
        self.plotted_axis.set_ylabel("value")
        self.plotted_axis.set_ylim(min_v, max_v)
        self.plotted_axis.set_title("Absolute Series Expenses")
        self.plotted_axis.legend()

    def normalized_series_expenses(self, transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :
        pass

    def absolute_total_expenses(self, transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :

        totals = {}
        for name, series_data in transaction_groups.items() :
            totals[name] = sum(series_data["balance"])

        min_v = min(totals.values())
        max_v = max(totals.values())

        self.plotted_axis.bar(totals.keys(), totals.values(), color='blue', alpha=0.7)
        self.plotted_axis.set_xlabel("Account")
        self.plotted_axis.set_ylabel("Amount")
        self.plotted_axis.set_ylim(min_v, max_v)
        self.plotted_axis.set_title("Absolute Total Expenses")

    def normalized_total_expenses(self, transaction_groups : TransactionGroupDict, from_timestamp : float, to_timestamp : float) -> None :
        pass

    def make_plot(self) :

        assert self.absolute_scale_radio.active != self.normalized_scale_radio.active
        is_absolute = self.absolute_scale_radio.active

        assert self.series_scale_radio.active != self.total_scale_radio.active
        is_series = self.series_scale_radio.active

        from_time_point = datetime.datetime.strptime(self.from_date_textbox.text, "%Y-%b-%d").timestamp()
        to_time_point = datetime.datetime.strptime(self.to_date_textbox.text, "%Y-%b-%d").timestamp()

        if from_time_point <= to_time_point :

            transaction_groups = get_selected_account_sets(self.ledger, self.tree_view_widget, from_time_point, to_time_point)

            self.plotted_figure, self.plotted_axis = plot_system.subplots()
            if is_series :
                if is_absolute :
                    self.absolute_series_expenses(transaction_groups, from_time_point, to_time_point)
                else :
                    self.normalized_series_expenses(transaction_groups, from_time_point, to_time_point)
            else :
                if is_absolute :
                    self.absolute_total_expenses(transaction_groups, from_time_point, to_time_point)
                else :
                    self.normalized_total_expenses(transaction_groups, from_time_point, to_time_point)
            buffer_data, buffer_size = self.plotted_figure.canvas.print_to_buffer()
            self.figure_container.set_texture(buffer_data, buffer_size)

    def __toggle_subtree(self, root_node) :
        is_active = root_node.toggle_inclusion.active
        for node in self.tree_view_widget.get_all_subtree_nodes(root_node) :
            node.toggle_inclusion.active = is_active
            
        

class AccountViewer(Screen) :

    account_name_label = ObjectProperty(None)
    account_data_table = ObjectProperty(None)

    def __init__(self, account_name : str, account_data : DataFrame, column_relative_sizes : typing.List[float], **kwargs : typing.ParamSpecKwargs) -> None :
        super(AccountViewer, self).__init__(**kwargs)

        self.account_name_label.text = account_name
        self.account_data_table.set_data_frame(account_data, column_relative_sizes)

    def filter_by_description(self, match_string : str) -> None :
        if match_string == "" :
            self.account_data_table.filter_by(lambda df : df)
        else :
            self.account_data_table.filter_by(lambda df : df[df['Description'].str.contains(match_string, regex=False)])

class StockedUpAppManager(ScreenManager) :

    def __init__(self, data_dir : Path, ledger_config : typing.Any, **kwargs : typing.ParamSpecKwargs) :
        super(StockedUpAppManager, self).__init__(transition=WipeTransition(), **kwargs)
        
        self.data_root_directory = data_dir
        self.ledger_configuration = ledger_config

        self.__overlay_stack : typing.List[Screen] = []
        self.__ledgers : typing.Dict[str, Ledger] = {}

    def swap_screen(self, screen_name : str) -> typing.Any :
        Logger.info(f"[StockedUpAppManager] swap_screen {screen_name}")
        if len(self.__overlay_stack) > 1 :
            Logger.error(f"Failed, currently has overlays! {[s.name for s in self.__overlay_stack]}")
            return None
        elif self.has_screen(screen_name) :
            if len(self.__overlay_stack) > 0 :
                Logger.info(f"Previous = {self.__overlay_stack[-1].name}")
                assert self.current == self.__overlay_stack[-1].name
                self.__overlay_stack.pop()
            else :
                Logger.info(f"First screen pushed, so no previous, current = {self.current}")
            screen = self.get_screen(screen_name)
            return self.push_overlay(screen)
        else :
            Logger.info(f"Failed, screen {screen_name} does not exist!")
            return None

    def push_overlay(self, screen : Screen) -> typing.Any :
        Logger.info(f"[StockedUpAppManager] push_overlay {self.current} -> {screen.name}")
        self.__overlay_stack.append(screen)
        return self.switch_to(screen, direction="left")

    def pop_overlay(self) -> typing.Any :
        assert len(self.__overlay_stack) > 1, "No overlays on stack!"
        assert self.current == self.__overlay_stack[-1].name
        Logger.info(f"[StockedUpAppManager] pop_overlay {self.current} -> {self.__overlay_stack[-2].name}")
        next_screen = self.switch_to(self.__overlay_stack[-2], direction="left")
        self.remove_widget(self.__overlay_stack.pop())
        return next_screen

    def import_ledger(self, ledger_import : LedgerImport) -> None :
        assert ledger_import.name not in self.__ledgers
        ledger_data_path = self.__get_ledger_path(ledger_import.name)
        if not ledger_data_path.exists() :
            Logger.info(f"Creating ledger folder {ledger_data_path}")
            ledger_data_path.mkdir()
        self.__ledgers[ledger_import.name] = Ledger(ledger_data_path, ledger_import)

    def import_ledgers(self) :
        Logger.info("[StockedUpAppManager] import_ledgers called, only importing FIRST ledger")
        if len(self.ledger_configuration.ledgers) == 0 :
            Logger.warning("No ledgers to import!")
            return
        #for ledger_import in self.ledger_configuration.ledgers :
        self.import_ledger(self.ledger_configuration.ledgers[0])
            
        self.load_default_ledger()

    def load_ledger(self, ledger_name : str) -> None :
        if not (ledger_name in self.__ledgers) :
            for ledger_import in self.ledger_configuration.ledgers :
                if ledger_import.name == ledger_name :
                    self.import_ledger(ledger_import)

        ledger = self.__ledgers[ledger_name]

        ledger_viewer = LedgerViewer()
        ledger_viewer.set_ledger(ledger)
        self.push_overlay(ledger_viewer)

    def load_default_ledger(self) :
        self.load_ledger(self.ledger_configuration.default_ledger)

    def __get_ledger_path(self, ledger_name : str) -> Path :
        return self.data_root_directory.joinpath(ledger_name)

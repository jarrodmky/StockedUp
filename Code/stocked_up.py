import pathlib
import typing
import argparse

from kivy.app import App
from kivy.core.window import Window
from kivy.metrics import mm

from UI.app_manager import StockedUpAppManager, kivy_initialize
from PyJMy.debug import debug_assert, debug_message

from type_check import run_type_check

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

        screen_manager = StockedUpAppManager(self.data_root_directory)

        screen_manager.swap_screen("LedgerSetup")
        return screen_manager

if __name__ == "__main__" :

    parser = argparse.ArgumentParser(description="An accounting tool that can read CSVs, categorize accounts and other analysis")
    parser.add_argument("--data_directory", nargs=1, required=True, help="Root directory for ledger data and configuration settings", metavar="<Data Directory>", dest="data_directory")
    parser.add_argument("--type_check", action="store_true", default=False, required=False, help="Run type check before execution", dest="type_check")

    arguments = parser.parse_args()

    debug_assert(isinstance(arguments.data_directory, list) and len(arguments.data_directory) == 1)
    data_root_directory = pathlib.Path(arguments.data_directory[0])

    if arguments.type_check and not run_type_check() :
        raise RuntimeError("Type check run failed!")
    else :
        kivy_initialize()
        StockedUpApp(data_root_directory).run()

import pathlib
import typing
import logging

from Code.UI.textureviewer import TextureViewer
from Code.UI.dataframetable import DataFrameTable

from kivy import require as version_require
from kivy.config import Config
from kivy.app import App
from kivy.core.window import Window
from kivy.metrics import mm
from kivy.logger import add_kivy_handlers, Logger
from kivy.lang.builder import Builder

Builder.load_file("Code/UI/kv/stockedup.kv")

from Code.UI.app_manager import StockedUpAppManager

def kivy_initialize() :
    version_require('2.0.0')
    Config.set('input', 'mouse', 'mouse')
    global_logger = logging.getLogger()
    add_kivy_handlers(global_logger)

class StockedUpApp(App) :
        
    kv_directory = "UI/kv"

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
        Logger.info("[StockedUpApp] build fired")

        self.scroll_bar_colour = [0.2, 0.7, 0.9, .5]
        self.scroll_bar_inactive_colour = [0.2, 0.7, 0.9, .5]
        self.scroll_bar_width = mm(4)
        self.fixed_button_height = mm(8)
        
        self.account_viewer_fixed_size = (3 * self.fixed_button_height + mm(6))
        self.account_viewer_fixed_row_height = 12

        screen_manager = StockedUpAppManager(self.data_root_directory)

        screen_manager.swap_screen("LedgerSetup")
        return screen_manager

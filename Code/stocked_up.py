import pathlib
import argparse

from stocked_up_app import StockedUpApp, kivy_initialize
from debug import debug_assert

if __name__ == '__main__' :

    parser = argparse.ArgumentParser(description="An accounting tool that can read CSVs, categorize accounts and other analysis")
    parser.add_argument("--data_directory", nargs=1, required=True, help="Root directory for ledger data and configuration settings", metavar="<Data Directory>", dest="data_directory")

    arguments = parser.parse_args()

    debug_assert(isinstance(arguments.data_directory, list) and len(arguments.data_directory) == 1)
    data_root_directory = pathlib.Path(arguments.data_directory[0])

    kivy_initialize()
    StockedUpApp(data_root_directory).run()

import pathlib
import argparse
import cProfile, pstats

from Code.logger import get_logger
logger = get_logger(__name__)

from Code.stockedupapp import StockedUpApp, kivy_initialize
from Code.type_check import run_type_check
    
def guarded_app_run(data_root_directory) :
    kivy_initialize()
    try :
        StockedUpApp(data_root_directory).run()
    except Exception as e :
        print(f"Hit exception when running StockedUp: {e}")


def main(type_check, profile, data_root_directory) :
    if type_check and not run_type_check() :
        return

    if profile :
        profiler = cProfile.Profile()
        profiler.enable()
        guarded_app_run(data_root_directory)
        profiler.disable()
        
        with open("./PROFILER_RESULTS.txt", "w") as f :
            results = pstats.Stats(profiler, stream=f)
            results.sort_stats(pstats.SortKey.CALLS)
            results.print_stats()
    else :
        guarded_app_run(data_root_directory)


if __name__ == "__main__" :

    parser = argparse.ArgumentParser(description="An accounting tool that can read CSVs, categorize accounts and other analysis")
    parser.add_argument("--data_directory", nargs=1, required=True, help="Root directory for ledger data and configuration settings", metavar="<Data Directory>", dest="data_directory")
    parser.add_argument("--type_check", action="store_true", default=False, required=False, help="Run type check before execution", dest="type_check")
    parser.add_argument("--profile", action="store_true", default=False, required=False, help="Print stats on function calls and time", dest="profile")

    arguments = parser.parse_args()

    main(arguments.type_check, arguments.profile, pathlib.Path(arguments.data_directory[0]))

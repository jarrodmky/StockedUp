import argparse

from Code.stocked_up_servers import guarded_server_run

if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description="Runs prefect server and serves pipeline flows")
    parser.add_argument("--with_tests", action="store_true", default=False, required=False, help="Serve testing flows")

    arguments = parser.parse_args()

    guarded_server_run(arguments.with_tests)

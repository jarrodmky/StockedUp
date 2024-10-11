import argparse
from prefect import serve

from Code.Pipeline import get_flows

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description="Runs prefect server pipeline flows")
    arguments = parser.parse_args()

    try :
        data_flows = get_flows()
        deployments = [data_flow.to_deployment("pipeline") for data_flow in data_flows]
        serve(*deployments)
    except Exception as e :
        if e is not KeyboardInterrupt :
            logger.info(f"Exception when running servers: {e}")
        else :
            logger.info("Server stopped!")

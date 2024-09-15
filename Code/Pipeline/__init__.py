from .csv_importing import read_transactions_from_csv_in_path

import subprocess
from threading import Thread
from prefect import serve

import logging
logger = logging.getLogger(__name__)

def serve_flows() :

    read_transactions = read_transactions_from_csv_in_path.to_deployment(name="read_transactions_from_csv_in_path")
    serve(read_transactions)

class PrefectServer :

    _Server = None

    def start() :
        PrefectServer._Server = subprocess.Popen(["prefect", "server", "start"], shell=True)
        if PrefectServer.is_running() :
            logger.info("Prefect server started!")
        else : 
            logger.info("Prefect server failed to start!")

    def is_running() -> bool :
        return PrefectServer._Server is not None
    
    def stop() :
        if PrefectServer.is_running() :
            PrefectServer._Server.terminate()
            PrefectServer._Server.wait()

class PipelineServer :

    _Server = None

    def start() :
        PipelineServer._Server = Thread(target=serve_flows)
        PipelineServer._Server.start()
        if PipelineServer.is_running() :
            logger.info("Pipeline server started!")
        else : 
            logger.info("Pipeline server failed to start!")

    def is_running() -> bool :
        return PipelineServer._Server is not None and PipelineServer._Server.is_alive()
    
    def stop() :
        if PipelineServer.is_running() :
            PipelineServer._Server.join()

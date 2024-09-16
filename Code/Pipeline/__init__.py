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

    @staticmethod
    def start() :
        PrefectServer._Server = subprocess.Popen(["prefect", "server", "start"], shell=True)
        if PrefectServer.is_running() :
            logger.info("Prefect server started!")
        else : 
            logger.info("Prefect server failed to start!")
            PrefectServer._Server = None

    @staticmethod
    def is_running() -> bool :
        return PrefectServer._Server is not None
    
    @staticmethod
    def stop() :
        if PrefectServer._Server is not None :
            server : subprocess.Popen = PrefectServer._Server
            server.terminate()
            server.wait()
            PrefectServer._Server = None

class PipelineServer :

    _Server = Thread()

    @staticmethod
    def start() :
        PipelineServer._Server = Thread(target=serve_flows)
        PipelineServer._Server.start()
        if PipelineServer.is_running() :
            logger.info("Pipeline server started!")
        else : 
            logger.info("Pipeline server failed to start!")
            PipelineServer._Server = Thread()

    @staticmethod
    def is_running() -> bool :
        return PipelineServer._Server.is_alive()
    
    @staticmethod
    def stop() :
        if PipelineServer._Server.is_alive() :
            PipelineServer._Server.join()

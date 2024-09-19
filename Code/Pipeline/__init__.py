from .account_importing import import_raw_accounts, import_ledger_source_accounts

from prefect import flow

import subprocess
from threading import Thread
from prefect import serve
from pathlib import Path

from Code.accounting_objects import AccountImport

from Code.logger import get_logger
logger = get_logger(__name__)

@flow
def test_import_raw_accounts() :
    test_import = AccountImport()
    test_import.folder = ".\RawAccounts\VCCU_CHEQUE"
    test_import.opening_balance = 2.93
    import_raw_accounts([test_import], Path(".\Data"))

@flow
def test_import_ledger_source_accounts() :
    import_ledger_source_accounts(Path(".\Data\LedgerConfiguration.json"), Path(".\Data"), "SomeLedger")

def serve_flows(serve_tests : bool) -> None :
    deployment_name = "pipeline"
    deployments = []
    if serve_tests :
        deployments.append(test_import_raw_accounts.to_deployment(deployment_name))
        deployments.append(test_import_ledger_source_accounts.to_deployment(deployment_name))
    deployments.append(import_raw_accounts.to_deployment(deployment_name))
    deployments.append(import_ledger_source_accounts.to_deployment(deployment_name))
    serve(*deployments)

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
    def start(serve_tests : bool) -> None :
        PipelineServer._Server = Thread(target=lambda : serve_flows(serve_tests))
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
    def stop() -> None :
        if PipelineServer._Server.is_alive() :
            PipelineServer._Server.join()

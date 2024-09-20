import typing
import time
import subprocess
from threading import Thread
from pathlib import Path

from .account_importing import import_raw_accounts, import_ledger_source_accounts

from prefect import flow, Flow
from prefect import serve

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

def get_flows(serve_tests : bool) -> typing.List[Flow] :
    deployments = []
    if serve_tests :
        deployments.append(test_import_raw_accounts)
        deployments.append(test_import_ledger_source_accounts)
    deployments.append(import_raw_accounts)
    deployments.append(import_ledger_source_accounts)
    return deployments

class PipelineServer :

    def start(self, serve_tests : bool) -> None :
        self.prefect_server = subprocess.Popen(["prefect", "server", "start"], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        time.sleep(1)
        
        if self.__prefect_server_is_running() :
            logger.info("Prefect server started!")
        else :
            logger.info("Prefect server failed to start!")
            self.prefect_server = None
            return

        time.sleep(3)

        data_flows = get_flows(serve_tests)
        deployments = [data_flow.to_deployment("pipeline") for data_flow in data_flows]
        self.deployment_server = Thread(target=lambda : serve(*deployments))
        self.deployment_server.start()
        if self.__deployment_server_is_running() :
            logger.info("Pipeline server started!")
        else : 
            logger.info("Pipeline server failed to start!")
            self.deployment_server = Thread()

    def __prefect_server_is_running(self) -> bool :
        return self.prefect_server is not None and self.prefect_server.poll() is None

    def __deployment_server_is_running(self) -> bool :
        return self.deployment_server is not None and self.deployment_server.is_alive()

    def is_running(self) -> bool :
        return self.__prefect_server_is_running() and self.__deployment_server_is_running()
    
    def stop(self) -> None :
        if self.__deployment_server_is_running() :
            self.deployment_server.join()

        time.sleep(3)

        if self.__prefect_server_is_running() :
            server : subprocess.Popen = self.prefect_server
            server.terminate()
            server.wait()
            self.prefect_server = None

import typing
from threading import Thread
from pathlib import Path

from .account_importing import import_raw_accounts, import_ledger_source_accounts

from prefect import flow, serve

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

def get_flows(serve_tests : bool) -> typing.List[typing.Any] :
    deployments : typing.List[typing.Any] = []
    if serve_tests :
        logger.info("Serving test flows")
        deployments.append(test_import_raw_accounts)
        deployments.append(test_import_ledger_source_accounts)
    deployments.append(import_raw_accounts)
    deployments.append(import_ledger_source_accounts)
    return deployments

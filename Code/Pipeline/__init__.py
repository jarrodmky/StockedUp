import typing

from .account_importing import get_imported_account
from .account_derivation import create_derived_accounts, create_derived_ledger_entries, verify_account_correspondence

from Code.logger import get_logger
logger = get_logger(__name__)

def get_flows() -> typing.List[typing.Any] :
    deployments : typing.List[typing.Any] = []
    deployments.append(get_imported_account)
    deployments.append(create_derived_accounts)
    deployments.append(create_derived_ledger_entries)
    deployments.append(verify_account_correspondence)
    return deployments

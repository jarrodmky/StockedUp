import argparse

from debug import debug_assert
from accounting import Ledger, data_path

parser = argparse.ArgumentParser(description="For a certain ledger, performs a mapping of transactions into derived accounts")
parser.add_argument("--ledger", nargs=1, required=True, help="The ledger the accounts are mapped in", metavar="<Ledger Name>", dest="ledger_name")

arguments = parser.parse_args()

debug_assert(isinstance(arguments.ledger_name, list) and len(arguments.ledger_name) == 1)
ledger_name = arguments.ledger_name[0]

ledger_data_path = data_path.joinpath(ledger_name)
if not ledger_data_path.exists() :
    assert(f"The ledger {ledger_name} does not exist!")

ledger = Ledger(ledger_data_path)
ledger.map_spending_accounts()

import typing
from pathlib import Path

from accounting import AccountManager, Transaction, ledger_data_path
from debug import debug_assert, debug_message
from json_file import json_read, json_register_readable
from utf8_file import utf8_file

account_mapping_file_path = ledger_data_path.joinpath("AccountMappings.json")
if not account_mapping_file_path.exists() :
    with utf8_file(account_mapping_file_path, 'x') as new_mapping_file :
        new_mapping_file.write("{\n")
        new_mapping_file.write("\t\"mappings\": []\n")
        new_mapping_file.write("}")

class AccountMapping :

    class Matching :

        def __init__(self) :
            pass

        @staticmethod
        def decode(reader) :
            new_matching = AccountMapping.Matching()
            new_matching.account_name = reader["account name"]
            new_matching.strings = reader["strings"]
            return new_matching

    def __init__(self) :
        pass

    @staticmethod
    def decode(reader) :
        new_account_mapping = AccountMapping()
        new_account_mapping.name = reader["name"]
        new_account_mapping.matchings = reader["matchings"]
        return new_account_mapping

class AccountMappingList :

    def __init__(self) :
        pass

    @staticmethod
    def decode(reader) :
        new_mapping_list = AccountMappingList()
        new_mapping_list.mappings = reader["mappings"]
        return new_mapping_list

json_register_readable(AccountMapping)
json_register_readable(AccountMapping.Matching)
json_register_readable(AccountMappingList)

account_mapping_list : typing.List[AccountMapping] = json_read(account_mapping_file_path).mappings
    
account_mapping_name_set : typing.Set[str] = set()

account_manager = AccountManager()

for account_mapping in account_mapping_list :
    debug_message(f"Mapping spending account \"{account_mapping.name}\"")
    account = account_manager.get_account_data(account_mapping.name)
    debug_assert(account is None, "Account already created! Can only map to new account")

    matching_transactions = []
    for matching in account_mapping.matchings :
        match_account = account_manager.get_account_data(matching.account_name)
        debug_assert(match_account is not None, "Account not found! Expected account \"" + matching.account_name + "\"")

        for matching_string in matching.strings :
            for transaction in match_account.transactions :
                if matching_string in transaction.description :
                    matching_transactions.append(Transaction(transaction.date, transaction.timestamp, -transaction.delta, transaction.description))

    account_manager.create_derived_account(account_mapping.name, matching_transactions)



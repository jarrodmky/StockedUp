import typing
from numpy import repeat
from polars import DataFrame, Series
from polars import concat
from prefect import task, flow
from prefect.cache_policies import TASK_SOURCE, INPUTS
from xxhash import xxh128

from Code.Utils.logger import get_logger
logger = get_logger(__name__)

from Code.source_database import SourceDataBase
from Code.Utils.hashing import hash_source, hash_object
from Code.Data.account_data import Account, DerivedAccount, InternalTransactionMapping, AccountMapping, ledger_columns

from Code.Pipeline.account_derivation import create_derived_matching_ledger_entries, get_matched_transactions

@task(cache_policy=TASK_SOURCE + INPUTS)
def create_derived_ledger_entries(account_derivations : typing.List[DerivedAccount], source_accounts : SourceDataBase) -> DataFrame :
    new_ledger_entries = []
    for account_derivation in account_derivations :
        derived_ledger_entries = create_derived_matching_ledger_entries(source_accounts, account_derivation)
        new_ledger_entries.append(derived_ledger_entries)
    return concat(new_ledger_entries)

@task(cache_policy=TASK_SOURCE + INPUTS)
def verify_account_correspondence(from_account : Account, to_account : Account, mapping : InternalTransactionMapping) -> DataFrame :
    
    from_matching_transactions = get_matched_transactions(from_account, mapping.from_match_strings)
    to_matching_transactions = get_matched_transactions(to_account, mapping.to_match_strings)

    from_account_name = from_account.name
    to_account_name = to_account.name

    #assumes in order on both accounts
    matched_length = min(from_matching_transactions.height, to_matching_transactions.height)
    from_matches_trunc = from_matching_transactions.head(matched_length)
    to_matches_trunc = to_matching_transactions.head(matched_length)
    if not from_matches_trunc["delta"].equals(-to_matches_trunc["delta"], strict=True) :
        logger.info(f"Not in sync! Tried:\n\t{from_account_name}\nTo:\n\t{to_account_name}")
          
    #print missing transactions
    print_missed_transactions = lambda name, data : logger.info(f"\"{name}\" missing {len(data)} transactions:\n{data.write_csv()}")
    diff_from_to = to_matching_transactions.height - from_matching_transactions.height
    if diff_from_to < 0 :
        print_missed_transactions(to_account_name, from_matching_transactions.tail(-diff_from_to))
    elif diff_from_to > 0 :
        print_missed_transactions(from_account_name, to_matching_transactions.tail(diff_from_to))
    if from_matching_transactions.height == 0 or to_matching_transactions.height == 0 :
        logger.info("... nothing to map!")
    else :
        logger.info("... account mapped!")
    
    internal_ledger_entries = DataFrame({
        "from_account_name" : repeat(from_account_name, matched_length),
        "from_transaction_id" : from_matches_trunc["ID"],
        "to_account_name" : repeat(to_account_name, matched_length),
        "to_transaction_id" : to_matches_trunc["ID"],
        "delta" : from_matches_trunc["delta"].abs()
    })
    return internal_ledger_entries
    
def verify_and_concat_ledger_entries(current_entries : DataFrame, new_ledger_entries : DataFrame) -> DataFrame :
    assert new_ledger_entries.columns == ledger_columns, "Incompatible columns detected!"
    new_ids = frozenset(concat([new_ledger_entries["from_transaction_id"], new_ledger_entries["to_transaction_id"]]))
    if len(new_ids) > 0 :
        current_accounted_ids = frozenset(concat([current_entries["from_transaction_id"], current_entries["to_transaction_id"]]))
        assert new_ids.isdisjoint(current_accounted_ids), f"Duplicate unique hashes already existing in ledger:\n{list(new_ids - current_accounted_ids)}\n, likely double matched!"
        return concat([current_entries, new_ledger_entries])
    return current_entries

def ledger_key(account_mapping : AccountMapping, source_accounts : SourceDataBase, task_source_object : typing.Any) -> str :
    hasher = xxh128()
    hash_object(hasher, account_mapping)
    for account in source_accounts.get_names() :
        hasher.update(source_accounts.get_account_hash(account))
    hash_source(hasher, task_source_object)
    return hasher.hexdigest()

def ledger_key_wrapper(run_context, parameters) :
    return ledger_key(
        parameters["account_mapping"], 
        parameters["source_accounts"], 
        run_context.task)

@task(cache_key_fn=ledger_key_wrapper)
def populate_ledger_entries(account_mapping : AccountMapping, source_accounts : SourceDataBase) -> DataFrame :
    derived_ledger_entries = create_derived_ledger_entries(account_mapping.derived_accounts, source_accounts)
    for mapping in account_mapping.internal_transactions :
        if mapping.from_account != mapping.to_account :
            logger.info(f"Mapping transactions from \"{mapping.from_account}\" to \"{mapping.to_account}\"")
            from_account = source_accounts.get_account(mapping.from_account)
            to_account = source_accounts.get_account(mapping.to_account)
            new_ledger_entries = verify_account_correspondence(from_account, to_account, mapping)
            derived_ledger_entries = verify_and_concat_ledger_entries(derived_ledger_entries, new_ledger_entries)
        else :
            logger.error(f"Transactions to same account {mapping.from_account}?")
    return derived_ledger_entries

def get_ledger_entries_hash(account_mapping : AccountMapping, source_accounts : SourceDataBase) -> str :
    return ledger_key(account_mapping, source_accounts, populate_ledger_entries)

@flow
def get_ledger_entries(account_mapping : AccountMapping, source_accounts : SourceDataBase) -> DataFrame :
    return populate_ledger_entries(account_mapping, source_accounts)

@task(cache_key_fn=ledger_key_wrapper)
def filter_unaccounted_transactions(account_mapping : AccountMapping, source_accounts : SourceDataBase) -> DataFrame :
    ledger_entries = get_ledger_entries(account_mapping, source_accounts)
    accounted_transaction_ids = DataFrame(Series("ID", list(concat([ledger_entries["from_transaction_id"], ledger_entries["to_transaction_id"]]))))
    unaccounted_transactions_data_frame_list = []
    source_accout_datas = [source_accounts.get_account(account_name) for account_name in source_accounts.get_names()]
    for account_data in source_accout_datas :
        unaccounted_dataframe = (account_data.transactions
            .join(accounted_transaction_ids, "ID", "anti")
            .select(["date", "description", "delta"]))
        account_column = Series("account", repeat(account_data.name, unaccounted_dataframe.height))
        unaccounted_dataframe = unaccounted_dataframe.insert_column(unaccounted_dataframe.width, account_column)
        unaccounted_transactions_data_frame_list.append(unaccounted_dataframe)
    if len(unaccounted_transactions_data_frame_list) > 0 :
        unaccounted_transactions = concat(unaccounted_transactions_data_frame_list)
        unaccounted_transactions = unaccounted_transactions.insert_column(0, Series("index", range(0, unaccounted_transactions.height)))
    return unaccounted_transactions

def get_unaccounted_transactions_hash(account_mapping : AccountMapping, source_accounts : SourceDataBase) -> str :
    return ledger_key(account_mapping, source_accounts, filter_unaccounted_transactions)

@flow
def get_unaccounted_transactions(account_mapping : AccountMapping, source_accounts : SourceDataBase) -> DataFrame :
    return filter_unaccounted_transactions(account_mapping, source_accounts)

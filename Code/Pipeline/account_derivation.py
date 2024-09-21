import typing
from pathlib import Path
from numpy import repeat
from polars import DataFrame, Series, String
from polars import concat, col
from prefect import task, flow
from prefect.cache_policies import TASK_SOURCE, INPUTS

from Code.PyJMy.json_file import json_read
from Code.PyJMy.utf8_file import utf8_file

from Code.logger import get_logger
logger = get_logger(__name__)

from Code.Data.account_data import Account, transaction_columns, DerivedAccount, InternalTransactionMapping
from Code.Data.hashing import make_identified_transaction_dataframe

AccountCache = typing.Dict[str, Account]

def escape_string(string : str) -> str :
    return string.replace("*", "\*").replace("+", "\+").replace("(", "\(").replace(")", "\)")

def strings_to_regex(strings : typing.List[str]) -> str :
    return "|".join([escape_string(s) for s in strings])

def derive_transaction_dataframe(account_name : str, dataframe : DataFrame) -> DataFrame :
    return DataFrame({
        "date" : dataframe["date"],
        "delta" : -dataframe["delta"],
        "description" : dataframe["description"],
        "timestamp" : dataframe["timestamp"],
        "source_ID" : dataframe["ID"],
        "source_account" : repeat(account_name, dataframe.height)
    })

@task(cache_policy=TASK_SOURCE + INPUTS)
def get_matched_transactions(match_account : Account, string_matches : typing.List[str]) -> DataFrame :
    account_name = match_account.name
    assert match_account is not None, f"Account not found! Expected account \"{account_name}\" to exist!"
    logger.info(f"Checking account {account_name} with {len(match_account.transactions)} transactions")
    
    regex = strings_to_regex(string_matches)
    matched_transactions = match_account.transactions.filter(col("description").str.contains(regex))

    logger.info(f"Found {matched_transactions.height} transactions in {account_name}")
    return matched_transactions

@task(cache_policy=TASK_SOURCE + INPUTS)
def get_derived_matched_transactions(source_account_cache : AccountCache, derived_account_mapping : DerivedAccount) -> DataFrame :
    matched_transaction_frames = []

    if len(derived_account_mapping.matchings) == 1 and derived_account_mapping.matchings[0] is not None and derived_account_mapping.matchings[0].account_name == "" :
        universal_match_strings = derived_account_mapping.matchings[0].strings
        logger.info(f"Checking all base accounts for {universal_match_strings}")
        sorted_source_keys = sorted(source_account_cache.keys())
        for account_name in sorted_source_keys :
            found_tuples = get_matched_transactions(source_account_cache[account_name], universal_match_strings)
            matched_transaction_frames.append(derive_transaction_dataframe(account_name, found_tuples))
            
    else :
        for matching in derived_account_mapping.matchings :
            if matching.account_name == "" :
                raise RuntimeError(f"Nonspecific match strings detected for account {derived_account_mapping.name}! Not compatible with specified accounts!")
            logger.info(f"Checking {matching.account_name} account for {matching.strings}")
            found_tuples = get_matched_transactions(source_account_cache[matching.account_name], matching.strings)
            matched_transaction_frames.append(derive_transaction_dataframe(matching.account_name, found_tuples))
    
    all_matched_transactions = concat(matched_transaction_frames)
    return all_matched_transactions.sort(by="timestamp", maintain_order=True)

AccountLedgerEntries = typing.Tuple[Account, DataFrame]

@task(cache_policy=TASK_SOURCE + INPUTS)
def create_derived_account(source_account_cache : AccountCache, account_derivation : DerivedAccount) -> AccountLedgerEntries :
    account_name = account_derivation.name
    derived_transactions = get_derived_matched_transactions(source_account_cache, account_derivation)
    assert account_name not in derived_transactions["source_account"].unique(), "Transaction to same account forbidden!"
    derived_transactions = make_identified_transaction_dataframe(derived_transactions)

    derived_ledger_entries = DataFrame({
        "from_account_name" : derived_transactions["source_account"],
        "from_transaction_id" : derived_transactions["source_ID"],
        "to_account_name" : Series(values=repeat(account_name, derived_transactions.height), dtype=String),
        "to_transaction_id" : derived_transactions["ID"],
        "delta" : derived_transactions["delta"].abs()
    })
    
    account = Account(account_name, account_derivation.start_value, derived_transactions[transaction_columns])
    return (account, derived_ledger_entries)

DerivationResults = typing.Tuple[typing.List[Account], DataFrame]

@flow
def create_derived_accounts(account_derivations : typing.List[DerivedAccount], source_account_cache : AccountCache) -> typing.Tuple[typing.List[Account], DataFrame] :
    derived_accounts = []
    new_ledger_entries = []
    for account_derivation in account_derivations :
        logger.info(f"Mapping spending account \"{account_derivation.name}\"")
        account, derived_ledger_entries = create_derived_account(source_account_cache, account_derivation)
        if len(account.transactions) > 0 :
            derived_accounts.append(account)
            new_ledger_entries.append(derived_ledger_entries)
            logger.info(f"... account {account_derivation.name} derived!")
        else :
            logger.info(f"... nothing to map for {account_derivation.name}!")
    return (derived_accounts, concat(new_ledger_entries))

@flow
def verify_account_correspondence(account_cache : AccountCache, mapping : InternalTransactionMapping) -> DataFrame :
    from_account = account_cache[mapping.from_account]
    to_account = account_cache[mapping.to_account]
    
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

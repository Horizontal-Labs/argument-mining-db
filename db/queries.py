"""
queries.py

This module provides functions to fetch claims and related premise data with an optional local cache layer.
If the cache file exists and the DB has no new rows, the results are loaded from cache. Otherwise, fresh data
is fetched from the DB and dumped into the cache for subsequent runs.
Cache behavior is controlled by the `CACHE_ENABLED` flag in the project's `config.py`.
"""
import pickle
from pathlib import Path
from .db import get_session
from .models import ADU, Relationship
from .config import CACHE_ENABLED
from collections import defaultdict

# --- Cache configuration ---
CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"
CACHE_DIR.mkdir(exist_ok=True)
CLAIMS_CACHE_FILE = CACHE_DIR / 'claims_cache.pkl'
DATA_CACHE_FILE   = CACHE_DIR / 'data_cache.pkl'

# --- Internal helpers ---

def _make_claims_query(session):
    """Return a base query for all claims ordered by ID."""
    return (
        session
        .query(ADU)
        .filter(ADU.type == 'claim')
        .order_by(ADU.id)
    )


def _get_claims(session, split: str = 'training') -> list[ADU]:
    """
    Get claims split into 'training', 'test', or 'benchmark'.
    """
    q = _make_claims_query(session)
    total = q.count()
    training_split = int(total * 0.7)
    test_split = int(total * 0.9)

    if split == 'training':
        return q.limit(training_split).all()
    elif split == 'test':
        return q.offset(training_split).limit(test_split - training_split).all()
    elif split == 'benchmark':
        return q.offset(test_split).all()
    else:
        raise ValueError("Invalid split. Must be 'training', 'test', or 'benchmark'")


def _get_data(session, split: str = 'training') -> tuple[list[ADU], list[ADU], list[str]]:
    """Get tuples of (claims, premises, relationship_category (stance_pro/stance_con) )."""

    # Fetch claims
    initial_claims = _get_claims(session, split)
    ids = [c.id for c in initial_claims]

    # Fetch related premises
    rows = (
        session
        .query(ADU, Relationship.category, Relationship.to_adu_id)
        .join(Relationship, Relationship.from_adu_id == ADU.id)
        .filter(ADU.type == 'premise', Relationship.to_adu_id.in_(ids))
        .all()
    )

    # If rows is empty, it means no premises were found for any of the initial_claims.
    # However, we need to group to see which specific claims have premises.
    premises, categories, claim_ids = zip(*rows) if rows else ([], [], [])

    # Group premises by claim_id
    grouped_premises_by_claim_id = defaultdict(list)
    for p, cat, cid in zip(premises, categories, claim_ids):
        grouped_premises_by_claim_id[cid].append((p, cat))


    # Prepare final lists for output
    output_claims = []
    output_premises = []
    output_categories = []

    # Alternate stances
    relationship_options = ['stance_pro', 'stance_con']
    
    # Iterate through the initially fetched claims and filter those without premises
    for claim_obj in initial_claims:
        if claim_obj.id in grouped_premises_by_claim_id:
            # This claim has premises, so include it
            output_claims.append(claim_obj)

            # Determine the desired stance based on the count of *included* claims.
            # Original code's enumerate(claims, start=1) means idx % 2 gives:
            # idx=1 (1st item) -> 1%2=1 (e.g., stance_con if options are [pro, con])
            # idx=2 (2nd item) -> 2%2=0 (e.g., stance_pro)
            # We use len(output_claims) which is 1-based for the current count.
            desired_stance_idx = len(output_claims) % 2 
            desired_stance = relationship_options[desired_stance_idx]

            # Get all premises for this specific claim
            current_claim_premises_list = grouped_premises_by_claim_id[claim_obj.id]
            
            # Try to find a premise with the desired stance
            chosen_premise_tuple = next(
                ((p, cat) for p, cat in current_claim_premises_list if cat == desired_stance),
                None
            )

            if chosen_premise_tuple is None:
                # If the desired stance wasn't found, pick the first available premise for this claim.
                # We know current_claim_premises_list is not empty because claim_obj.id is in grouped_premises_by_claim_id.
                chosen_premise_tuple = current_claim_premises_list[0]
            
            output_premises.append(chosen_premise_tuple[0])   # The ADU object of the premise
            output_categories.append(chosen_premise_tuple[1]) # The category string
            
    return output_claims, output_premises, output_categories


def _load_cache(file_path: Path):
    """Load cached data from `file_path`, or return None if missing/corrupt."""
    try:
        if file_path.exists():
            with open(file_path, 'rb') as f:
                return pickle.load(f)
    except Exception:
        pass
    return None


def _save_cache(file_path: Path, data):
    """Dump `data` to `file_path` via pickle."""
    with open(file_path, 'wb') as f:
        pickle.dump(data, f)

# --- Public API ---

def get_training_claims() -> list[ADU]:
    """Return the first 70% of claims, using cache if enabled and up-to-date."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_claims(session, training=True)
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(CLAIMS_CACHE_FILE)
        if cache and cache.get('total') == total:
            return cache['training']
        training = _get_claims(session, split='training')
        test_ = _get_claims(session, split='test')
        benchmark = _get_claims(session, split='benchmark')
        _save_cache(CLAIMS_CACHE_FILE, {'total': total, 'training': training, 'test': test_, 'benchmark': benchmark})
        return training


def get_test_claims() -> list[ADU]:
    """Return the second 20% of claims, using cache if enabled and up-to-date."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_claims(session, training=False)
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(CLAIMS_CACHE_FILE)
        if cache and cache.get('total') == total:
            return cache['test']
        training = _get_claims(session, split='training')
        test_ = _get_claims(session, split='test')
        benchmark = _get_claims(session, split='benchmark')
        _save_cache(CLAIMS_CACHE_FILE, {'total': total, 'training': training, 'test': test_, 'benchmark': benchmark})
        return test_

def get_benchmark_claims() -> list[ADU]:
    """Return the last 10% of claims, using cache if enabled and up-to-date."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_claims(session, split='benchmark')
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(CLAIMS_CACHE_FILE)
        if cache and cache.get('total') == total and 'benchmark' in cache:
            return cache['benchmark']
        training = _get_claims(session, split='training')
        test_ = _get_claims(session, split='test')
        benchmark = _get_claims(session, split='benchmark')
        _save_cache(CLAIMS_CACHE_FILE, {'total': total, 'training': training, 'test': test_, 'benchmark': benchmark})
        return benchmark
    
def get_training_data() -> tuple[list[ADU], list[ADU], list[str]]:
    """Return (claims, premises, relationship_category (stance_pro/stance_con) ) for training split, using cache if enabled and fresh."""

    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_data(session, training=True)
        db_total_claims = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(DATA_CACHE_FILE)
        if cache and cache.get('db_total_claims_when_cached') == db_total_claims: # More robust cache check
            return cache['training']
        
        train_ = _get_data(session, split='training')
        test_ = _get_data(session, split='test')
        benchmark_ = _get_data(session, split='benchmark')
        _save_cache(DATA_CACHE_FILE, {'db_total_claims_when_cached': db_total_claims, 'training': train_, 'test': test_, 'benchmark':benchmark_})
        return train_


def get_test_data() -> tuple[list[ADU], list[ADU], list[str]]:
    """Return (claims, premises, relationship_category (stance_pro/stance_con) ) for test split, using cache if enabled and fresh."""

    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_data(session, training=False)
        db_total_claims = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(DATA_CACHE_FILE)
        if cache and cache.get('db_total_claims_when_cached') == db_total_claims: # More robust cache check
            return cache['test']

        # If cache is invalid or test data not present, might need to re-fetch both if cache stores them together
        train_ = _get_data(session, split='training') # Or fetch only test if cache structure allows
        test_ = _get_data(session, split='test')
        benchmark_ = _get_data(session, split='benchmark')
        _save_cache(DATA_CACHE_FILE, {'db_total_claims_when_cached': db_total_claims, 'training': train_, 'test': test_, 'benchmark':benchmark_})
        return test_

def get_benchmark_data() -> tuple[list[ADU], list[ADU], list[str]]:
    """Return (claims, premises, relationship_category (stance_pro/stance_con) ) for benchmark split, using cache if enabled and fresh."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_data(session, split='benchmark')
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(DATA_CACHE_FILE)
        if cache and cache.get('db_total_claims_when_cached') == total and 'benchmark' in cache:
            return cache['benchmark']
        train_ = _get_data(session, split='training')
        test_ = _get_data(session, split='test')
        benchmark_ = _get_data(session, split='benchmark')
        _save_cache(DATA_CACHE_FILE, {'db_total_claims_when_cached': total, 'training': train_, 'test': test_, 'benchmark': benchmark_})
        return benchmark_

def get_sharded_training_data(max_per_shard: int, num_shards: int) -> list[tuple[list[ADU], list[ADU], list[str]]]:
    """
    Return a list of `num_shards` tuples (claims, premises, categories), each containing
    exactly `max_per_shard` items, drawn consecutively from the training split.

    Raises:
        ValueError: if max_per_shard <= 0, num_shards <= 0,
                    or nicht genug Daten, um alle Shards komplett zu füllen.
    """
    if max_per_shard <= 0 or num_shards <= 0:
        raise ValueError("max_per_shard und num_shards müssen positive Ganzzahlen sein.")
    
    # Holt alle Trainingsdaten
    claims, premises, categories = get_training_data()
    total = len(claims)
    required = max_per_shard * num_shards
    
    if total < required:
        raise ValueError(
            f"Nicht genug Trainingsdaten: benötigt {required}, vorhanden {total}."
        )
    
    shards: list[tuple[list[ADU], list[ADU], list[str]]] = []
    for i in range(num_shards):
        start = i * max_per_shard
        end = start + max_per_shard
        # Die Slices haben hier garantiert die Länge max_per_shard
        shards.append((
            claims[start:end],
            premises[start:end],
            categories[start:end]
        ))
    
    return shards

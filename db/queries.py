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


def _get_claims(session, training: bool = True) -> list[ADU]:
    """Get claims split by training/test set, reusing the passed-in session."""
    q = _make_claims_query(session)
    total = q.count()
    split = int(total * 0.8)
    return q.limit(split).all() if training else q.offset(split).all()


def _get_data(session, training: bool = True) -> tuple[list[ADU], list[ADU], list[str]]:
    """Get tuples of (claims, alternating premises, relationships)."""
    # Fetch claims
    claims = _get_claims(session, training)
    ids = [c.id for c in claims]

    # Fetch related premises
    rows = (
        session
        .query(ADU, Relationship.category, Relationship.to_adu_id)
        .join(Relationship, Relationship.from_adu_id == ADU.id)
        .filter(ADU.type == 'premise', Relationship.to_adu_id.in_(ids))
        .all()
    )
    premises, categories, claim_ids = zip(*rows) if rows else ([], [], [])

    # Group by claim_id
    grouped = defaultdict(list)
    for p, cat, cid in zip(premises, categories, claim_ids):
        grouped[cid].append((p, cat))

    # Alternate stances
    relationship_options = ['stance_pro', 'stance_con']
    alt_premises = []
    alt_categories = []
    for idx, cl in enumerate(claims, start=1):
        desired = relationship_options[idx % 2]
        chosen = next(((p, cat) for p, cat in grouped.get(cl.id, []) if cat == desired), None)
        if chosen is None:
            chosen = grouped.get(cl.id, [(None, None)])[0]
        alt_premises.append(chosen[0])
        alt_categories.append(chosen[1])

    return claims, alt_premises, alt_categories


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
    """Return the first 80% of claims, using cache if enabled and up-to-date."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_claims(session, training=True)
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(CLAIMS_CACHE_FILE)
        if cache and cache.get('total') == total:
            return cache['training']
        training = _get_claims(session, training=True)
        test_    = _get_claims(session, training=False)
        _save_cache(CLAIMS_CACHE_FILE, {'total': total, 'training': training, 'test': test_})
        return training


def get_test_claims() -> list[ADU]:
    """Return the last 20% of claims, using cache if enabled and up-to-date."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_claims(session, training=False)
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(CLAIMS_CACHE_FILE)
        if cache and cache.get('total') == total:
            return cache['test']
        training = _get_claims(session, training=True)
        test_    = _get_claims(session, training=False)
        _save_cache(CLAIMS_CACHE_FILE, {'total': total, 'training': training, 'test': test_})
        return test_


def get_training_data() -> tuple[list[ADU], list[ADU], list[str]]:
    """Return (claims, premises, relationships) for training split, using cache if enabled and fresh."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_data(session, training=True)
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(DATA_CACHE_FILE)
        if cache and cache.get('total') == total:
            return cache['training']
        train_ = _get_data(session, training=True)
        test_  = _get_data(session, training=False)
        _save_cache(DATA_CACHE_FILE, {'total': total, 'training': train_, 'test': test_})
        return train_


def get_test_data() -> tuple[list[ADU], list[ADU], list[str]]:
    """Return (claims, premises, relationships) for test split, using cache if enabled and fresh."""
    with get_session() as session:
        if not CACHE_ENABLED:
            return _get_data(session, training=False)
        total = session.query(ADU).filter(ADU.type == 'claim').count()
        cache = _load_cache(DATA_CACHE_FILE)
        if cache and cache.get('total') == total:
            return cache['test']
        train_ = _get_data(session, training=True)
        test_  = _get_data(session, training=False)
        _save_cache(DATA_CACHE_FILE, {'total': total, 'training': train_, 'test': test_})
        return test_



print(str(get_training_claims()[5]))


train = get_training_data()
print(train[1][5])
print(train[2][5])

print('/n/n')
print(str(get_training_claims()[6]))
print(train[1][6])
print(train[2][6])
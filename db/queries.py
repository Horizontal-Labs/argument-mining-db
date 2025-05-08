from .db import get_session
from .models import ADU, Relationship
from collections import defaultdict
        

def _get_claims(training: bool = True) -> list[ADU]:
    """Get claims split by training/test set
    Args:
        training: If True, returns first 80% of claims, if False returns last 20%
    """
    with get_session() as session:
        adus = session.query(ADU)\
            .filter(ADU.type == 'claim')\
            .order_by(ADU.id)\
            .all()
        split_idx = int(len(adus) * 0.8)
        return adus[:split_idx] if training else adus[split_idx:]

def _get_data(training: bool = True) -> tuple[list[ADU], list[ADU], list[str]]:
    """Get data split by training/test set
    Args:
        training: If True, returns training data, if False returns test data
    Returns:
        tuple containing (claims, premises, relationships)
    """
    with get_session() as session:
        claims = _get_claims(training)
        ids = [c.id for c in claims]

        result = (
            session.query(ADU, Relationship.category, Relationship.to_adu_id)
            .join(Relationship, Relationship.from_adu_id == ADU.id)
            .filter(
                ADU.type == "premise",
                Relationship.to_adu_id.in_(ids)
            )
            .all()
        )

        premises = [row[0] for row in result]
        categories = [row[1] for row in result]
        claim_ids = [row[2] for row in result]

        # Preprocess to group by claim_id
        grouped = defaultdict(list)
        for p, to_id, r_type in zip(premises, claim_ids, categories):
            grouped[to_id].append((p, r_type))

        relationship_options = ['stance_pro', 'stance_con']
        alternating_premises = []
        alternating_relationships = []

        for i, cl in enumerate(claims, 1):
            c = cl.id
            found = False
            wished_relationship = relationship_options[i % 2]

            for p, r_type in grouped.get(c, []):
                if r_type == wished_relationship:
                    alternating_premises.append(p)
                    alternating_relationships.append(r_type)
                    found = True
                    break

            if not found:
                # fallback: still add one of the available items if exists
                fallback = grouped.get(c, [(None, None)])[0]
                alternating_premises.append(fallback[0])
                alternating_relationships.append(fallback[1])

        return claims, alternating_premises, alternating_relationships

# Wrapper functions to maintain the same interface
def get_training_claims() -> list[ADU]:
    """
    Returns:
    tuple containing (claims, premises, relationships)
    """
    return _get_claims(training=True)

def get_test_claims() -> list[ADU]:
    """
    Returns:
    tuple containing (claims, premises, relationships)
    """
    return _get_claims(training=False)

def get_training_data() -> tuple[list[ADU], list[ADU], list[str]]:
    """
    Returns:
    tuple containing (claims, premises, relationships)
    """
    return _get_data(training=True)

def get_test_data() -> tuple[list[ADU], list[ADU], list[str]]:
    """
    Returns:
    tuple containing (claims, premises, relationships)
    """
    return _get_data(training=False)

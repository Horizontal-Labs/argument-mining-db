import json
from db import models
from db.db import get_session

def add_topics(data: list[dict]) -> list[models.Domain]:
    """Extract unique topics and return Domain objects."""
    unique_topics = set()
    topic_cls = []
    for element in data:
        topic = element.get("conclusion")
        discussion_title = element.get("context", {}).get("discussionTitle")
        if topic != discussion_title:
            print("Topic mismatch -> Skipping")
            continue
        if topic in unique_topics:
            continue
        print(f"New topic found: {topic}")
        topic_cls.append(models.Domain(domain_name=topic))
        unique_topics.add(topic)
    return topic_cls

def add_adus(data: list[dict], domain_name_to_id: dict) -> list[models.ADU]:
    """Create ADU entries from premises and unique conclusions (claims)."""
    adus = []
    added_claims = set()  # Track claims we've already added

    for element in data:
        topic = element.get("conclusion")
        discussion_title = element.get("context", {}).get("discussionTitle")
        if topic != discussion_title:
            continue

        domain_id = domain_name_to_id.get(topic)
        if not domain_id:
            print(f"Missing domain ID for topic: {topic}")
            continue

        # Add each premise (duplicates are allowed)
        for premise in element.get("premises", []):
            text = premise.get("text")
            adus.append(models.ADU(
                text=text,
                type="premise",
                domain_id=domain_id
            ))

        # Only add the claim once (per unique topic text)
        if topic not in added_claims:
            adus.append(models.ADU(
                text=topic,
                type="claim",
                domain_id=domain_id
            ))
            added_claims.add(topic)

    return adus

def add_relationships(data: list[dict], adu_text_to_id: dict, domain_name_to_id: dict) -> list[models.Relationship]:
    """Create Relationship entries by linking premises to their claim using stance."""
    relationships = []
    for element in data:
        topic = element.get("conclusion")
        discussion_title = element.get("context", {}).get("discussionTitle")
        if topic != discussion_title:
            continue

        domain_id = domain_name_to_id.get(topic)
        if not domain_id:
            continue

        claim_adu_id = adu_text_to_id.get(topic)
        if not claim_adu_id:
            print(f"Claim ADU not found for topic: {topic}")
            continue

        for premise in element.get("premises", []):
            text = premise.get("text")
            stance = premise.get("stance")

            if stance == "PRO":
                category = "stance_pro"
            elif stance == "CON":
                category = "stance_con"
            else:
                print(f"Unknown stance for premise: {text}")
                continue

            premise_adu_id = adu_text_to_id.get(text)
            if not premise_adu_id:
                print(f"Premise ADU not found for text: {text}")
                continue

            relationships.append(models.Relationship(
                from_adu_id=premise_adu_id,
                to_adu_id=claim_adu_id,
                category=category,
                domain_id=domain_id
            ))

    return relationships

def commit_in_batches(session, objects, size=5000):
    """
    Bulk-save in chunks of size.
    """
    total = len(objects)
    batch_num = 1

    for i in range(0, total, size):
        batch = objects[i : i + size]
        start, end = i + 1, i + len(batch)

        print(f"[Batch {batch_num}] Bulk‐saving records {start}–{end} of {total}…")
        session.bulk_save_objects(batch)
        session.commit()
        print(f"[Batch {batch_num}] Committed {len(batch)} records.\n")

        batch_num += 1

    print("All batches committed.")

def main():
    # Load and sort data
    with open(r"args-me-1.0-cleaned.json", "r") as file: #NOTE: Could not update because the file is so big. Source: https://zenodo.org/records/4139439
        data = json.load(file)

    arguments = data["arguments"]
    sorted_data = sorted(arguments, key=lambda x: x.get("conclusion", "").lower())
    
    session = get_session()

    # Add Domains 
    domain_objs = add_topics(sorted_data)
    print(f"{len(domain_objs)} Topics are found in the dataset")

    session.add_all(domain_objs)
    session.commit()

    print("All domains are imported correctly")
    # Map domain names to their DB IDs
    domain_name_to_id = {
        domain.domain_name: domain.id for domain in session.query(models.Domain).all()
    }

    # Add ADUs 
    adu_objs = add_adus(sorted_data, domain_name_to_id)
    print(f"{len(adu_objs)} ADU Objects are found in the dataset")

    commit_in_batches(session,adu_objs)
    print("All ADUs are added correctly")

    # Map unique text or argument IDs to ADU IDs (for relationship mapping)
    adu_text_to_id = {
        adu.text: adu.id for adu in session.query(models.ADU).all()
    }

    # Add Relationships
    relationship_objs = add_relationships(sorted_data, adu_text_to_id, domain_name_to_id)
    print(f"{len(relationship_objs)} Relationships are found in the dataset")

    commit_in_batches(session,relationship_objs)
    print("All Relationships are added correctly")

if __name__ == "__main__":
    main()
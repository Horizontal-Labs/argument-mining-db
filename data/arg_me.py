import sys 
sys.path.append(r"C:\Users\ahmed\Desktop\Master_Studium\Workspace_Studium\Argument-Mining")
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
    """Create ADU entries from premises and conclusions."""
    adus = []
    for element in data:
        topic = element.get("conclusion")
        discussion_title = element.get("context", {}).get("discussionTitle")
        if topic != discussion_title:
            continue

        domain_id = domain_name_to_id.get(topic)
        if not domain_id:
            print(f"Missing domain ID for topic: {topic}")
            continue

        for idx, premise in enumerate(element.get("premises", [])):
            text = premise.get("text")
            stance = premise.get("stance")
            if stance == "PRO":
                adu_type = "stance_pro"
            elif stance == "CON":
                adu_type = "stance_con"
            else:
                print("Unknown stance type")
                continue

            adus.append(models.ADU(
                text=text,
                type=adu_type,
                domain_id=domain_id
            ))

        # Add the claim (conclusion) as a separate ADU
        adus.append(models.ADU(
            text=topic,
            type="claim",
            domain_id=domain_id
        ))

    return adus

def add_relationships(data: list[dict], adu_text_to_id: dict, domain_name_to_id: dict) -> list[models.Relationship]:
    """Create Relationship entries from previous/current argument links."""
    relationships = []
    for element in data:
        topic = element.get("conclusion")
        discussion_title = element.get("context", {}).get("discussionTitle")
        if topic != discussion_title:
            continue

        from_adu_key = element.get("context", {}).get("previousArgumentInSourceId")
        to_adu_key = element.get("id")

        from_adu_id = adu_text_to_id.get(from_adu_key)
        to_adu_id = adu_text_to_id.get(to_adu_key)

        if not from_adu_id or not to_adu_id:
            continue

        domain_id = domain_name_to_id.get(topic)
        if not domain_id:
            continue

        relationships.append(models.Relationship(
            from_adu_id=from_adu_id,
            to_adu_id=to_adu_id,
            category="support",  # could vary depending on stance logic
            domain_id=domain_id
        ))

    return relationships

def main():
    # Load and sort data
    with open(r"C:\Users\ahmed\Desktop\Master_Studium\GP\Data-Sets\arg-me\args-me-1.0-cleaned.json", "r") as file:
        data = json.load(file)

    arguments = data["arguments"]
    sorted_data = sorted(arguments, key=lambda x: x.get("conclusion", "").lower())
    
    session = get_session()

    # Add Domains 
    domain_objs = add_topics(sorted_data)
    session.add_all(domain_objs)
    session.commit()

    print("All domains are imported correctly")
    # Map domain names to their DB IDs
    domain_name_to_id = {
        domain.domain_name: domain.id for domain in session.query(models.Domain).all()
    }

    # Add ADUs 
    adu_objs = add_adus(sorted_data, domain_name_to_id)
    session.add_all(adu_objs)
    session.commit()
    
    print("All ADUs are added correctly")
    # Map unique text or argument IDs to ADU IDs (for relationship mapping)
    adu_text_to_id = {
        adu.text: adu.id for adu in session.query(models.ADU).all()
    }

    # Add Relationships
    relationship_objs = add_relationships(sorted_data, adu_text_to_id, domain_name_to_id)
    session.add_all(relationship_objs)
    session.commit()
    print("All Relationships are added correctly")

if __name__ == "__main__":
    main()
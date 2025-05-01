import sys
import os

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)

# adding the parent directory to 
# the sys.path.
sys.path.append(parent)
import pandas as pd
from ..db.db import get_session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ..db.models import Domain, ADU, Relationship, Base  # Replace 'your_module' with your actual module name



session = get_session()

FILE_PATH = current + "/claim_stance_dataset_v1.csv"

sample_data = {
    'topicId': [1],
    'topicText': ["This house believes that the sale of violent video games to minors should be banned"],
    'claims.claimCorrectedText': ["Exposure to violent video games causes at least a temporary increase in aggression and this exposure correlates with aggression in the real world"],
    'claims.stance': ["PRO"]
}

def read_csv(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file and returns a DataFrame.
    """
    return pd.read_csv(file_path)

if __name__ == "__main__":
    # Read the CSV file or use sample data
    df = read_csv(FILE_PATH)
    #df = pd.DataFrame(sample_data)
    exit(1)
    # Group the data by 'topicId'
    grouped = df.groupby('topicId')
    
    for topicId, group in grouped:
        # Step 1: Insert Domain
        domain_name = group['topicText'].iloc[0]
        domain = Domain(domain_name=domain_name)
        session.add(domain)
        session.commit()  # Commit to get the domain ID
        
        # Step 2: Insert Claim (ADU with type='claim')
        claim_text = domain_name  # Assuming topicText is the claim
        claim_adu = ADU(text=claim_text, type='claim', domain_id=domain.id)
        session.add(claim_adu)
        session.commit()  # Commit to get the claim ID
        
        # Step 3: Insert Premises (ADUs with type='premise') and Relationships
        for index, row in group.iterrows():
            premise_text = row['claims.claimCorrectedText']
            stance = row['claims.stance']
            
            # Insert Premise
            premise_adu = ADU(text=premise_text, type='premise', domain_id=domain.id)
            session.add(premise_adu)
            session.commit()  # Commit to get the premise ID
            
            # Determine relationship category
            if stance == "PRO":
                category = "support"
            elif stance == "CON":
                category = "stance_con"  # Adjusted to match your model's constraint
            else:
                category = "stance_pro"  # Default fallback, adjust as needed
            
            # Insert Relationship (from premise to claim)
            relationship = Relationship(
                from_adu_id=premise_adu.id,
                to_adu_id=claim_adu.id,
                category=category,
                domain_id=domain.id
            )
            session.add(relationship)
        
        session.commit()  # Final commit for the group
    
    # Close the session
    session.close()
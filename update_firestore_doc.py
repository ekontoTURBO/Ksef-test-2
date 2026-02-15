import os
from google.cloud import firestore

# Hardcode project ID or fetch from env
PROJECT_ID = "n8n-cognitra"
CLIENT_ID = "6430004340"
SPREADSHEET_ID = "1KIyLuWMb-nfoa7vlX1BYueIP8w61_kEfzuc3c0Un4ZQ"

def update_client_doc():
    print(f"Updating Firestore document {CLIENT_ID} in project {PROJECT_ID}...")
    db = firestore.Client(project=PROJECT_ID)
    
    doc_ref = db.collection("clients").document(CLIENT_ID)
    
    # Update field
    doc_ref.update({
        "spreadsheet_id": SPREADSHEET_ID
    })
    
    print(f"Successfully updated document {CLIENT_ID} with spreadsheet_id: {SPREADSHEET_ID}")

if __name__ == "__main__":
    update_client_doc()

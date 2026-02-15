from google.cloud import firestore
import logging

logging.basicConfig(level=logging.INFO)

def setup_firestore():
    project_id = "n8n-cognitra"
    db = firestore.Client(project=project_id)
    
    doc_ref = db.collection("clients").document("6430004340")
    
    data = {
        "client_name": "Restauracja Park Szwajcaria",
        "sheet_name": "Restauracja Park Szwajcaria KSEF",
        "boss_email": "cognitra.adm@gmail.com",
        "ksef_nip": "6430004340", # Using ksef_nip as per config_manager logic
        "nip": "6430004340",      # Adding both for compatibility
        "is_shared": False,
        "telegram_chat_id": "12345678"
    }
    
    doc_ref.set(data)
    logging.info(f"Document 6430004340 created/updated in project {project_id}")

if __name__ == "__main__":
    setup_firestore()

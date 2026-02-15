from google.cloud import firestore
import logging

class FirestoreConfig:
    def __init__(self, project_id=None):
        self.db = firestore.Client(project=project_id)
        self.collection_name = "clients"

    def get_active_clients(self):
        """
        Fetches all client documents.
        Returns a list of dicts containing client configuration.
        """
        clients_ref = self.db.collection(self.collection_name)
        # You might want to filter by 'active' status if you have such a field
        docs = clients_ref.stream()
        
        clients = []
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id  # Include document ID
            
            # Helper to safely get fields or defaults
            client_config = {
                "id": doc.id,
                "client_name": data.get("client_name"),
                "sheet_name": data.get("sheet_name"),
                "boss_email": data.get("boss_email"),
                "nip": data.get("ksef_nip") or data.get("nip"), # Support both naming conventions
                "execution_mode": data.get("execution_mode", "C"), # Default to Daily if not set
                "is_shared": data.get("is_shared", False),
                 # Added requested field
                "telegram_chat_id": data.get("telegram_chat_id"),
                "spreadsheet_id": data.get("spreadsheet_id")
            }
            
            # Validate essential fields
            if client_config["nip"] and client_config["sheet_name"]:
                clients.append(client_config)
            else:
                logging.warning(f"Skipping invalid client doc {doc.id}: missing nip or sheet_name")
                
        return clients

    def update_client_shared_status(self, client_id, is_shared=True):
        """
        Updates the 'is_shared' flag for a client.
        """
        doc_ref = self.db.collection(self.collection_name).document(client_id)
        doc_ref.update({"is_shared": is_shared})
        logging.info(f"Updated client {client_id} shared status to {is_shared}")

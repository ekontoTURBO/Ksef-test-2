from google.cloud import secretmanager
import os

class SecretsManager:
    def __init__(self, project_id=None):
        self.client = secretmanager.SecretManagerServiceClient()
        # If project_id is not provided, we can try to infer it or rely on fully qualified secret names
        # But constructing names requires project_id usually.
        # In Cloud Run, metadata server might provide it, but for simplicity let's require it or use env.
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")

    def get_secret(self, secret_id, version_id="latest"):
        """
        Fetches a secret payload from Secret Manager.
        secret_id: The name of the secret (e.g., 'ksef-token-1234567890')
        """
        if not self.project_id:
            raise ValueError("Project ID is required to fetch secrets.")

        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        
        try:
            response = self.client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"[ERROR] Failed to fetch secret {secret_id}: {e}")
            return None

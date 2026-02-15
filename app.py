import os
import logging
from flask import Flask, request
import main

app = Flask(__name__)

# Configure logging to show up in Cloud Logging
logging.basicConfig(level=logging.INFO)

@app.route("/", methods=["POST", "GET"])
def trigger_sync():
    """
    Triggers the main KSeF Sync logic.
    Cloud Scheduler will send a POST request here.
    """
    logging.info("Received Trigger for KSeF Sync.")
    try:
        # Ensure we are in Cloud Mode
        os.environ["EXECUTION_ENV"] = "CLOUD" 
        # Run the main cloud logic
        main.run_cloud_mode()
        return "Sync Complete", 200
    except Exception as e:
        logging.error(f"Error during sync: {e}")
        return f"Error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

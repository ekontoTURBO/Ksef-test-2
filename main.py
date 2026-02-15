import os
import sys
import json
import time
import argparse
import datetime
import logging
from dotenv import load_dotenv

# Import Clients and Managers
from ksef_client import KsefClient
from sheets_client import SheetsClient
from secrets_manager import SecretsManager
from config_manager import FirestoreConfig

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_base_url(env):
    env = env.upper()
    if env == "PROD":
        return "https://api.ksef.mf.gov.pl/api/v2"
    elif env == "DEMO":
        return "https://ksef-demo.mf.gov.pl/api/online"
    else: # TEST
        return "https://ksef-test.mf.gov.pl/api/online"

def process_single_client(client_config, secrets_mgr, sheets_creds, ksef_env="PROD"):
    """
    Processes a single client: Syncs KSeF data to Google Sheets.
    """
    client_name = client_config["client_name"]
    sheet_name = client_config["sheet_name"]
    nip = client_config["nip"]
    boss_email = client_config["boss_email"]
    client_id = client_config["id"]
    spreadsheet_id = client_config.get("spreadsheet_id")
    
    logging.info(f"DEBUG: spreadsheet_id for {client_name} is '{spreadsheet_id}' (Type: {type(spreadsheet_id)})")
    
    logging.info(f"--- Processing Client: {client_name} (NIP: {nip}) ---")

    # 1. Fetch KSeF Token from Secrets
    # Secret Name: ksef-token-{NIP}
    secret_name = f"ksef-token-{nip}"
    ksef_token = secrets_mgr.get_secret(secret_name)
    
    if not ksef_token:
        logging.error(f"Could not fetch KSeF token for {client_name} (Secret: {secret_name}). Skipping.")
        return

    # Sanitize Token (Take part before '|' and strip whitespace)
    ksef_token = ksef_token.split("|")[0].strip()
    logging.info(f"Sanitized Token (first 5 chars): {ksef_token[:5]}*****")

    # 2. Init Sheets Client
    try:
        sheets = SheetsClient(sheets_creds, sheet_name, spreadsheet_id=spreadsheet_id)
        sheets.authenticate()
        sheets.get_or_create_sheet()
        
        # 3. Share Sheet if needed
        # Skip sharing if using an existing ID (presumed shared/owned by user)
        if spreadsheet_id:
             logging.info(f"Using existing Spreadsheet ID {spreadsheet_id}. Skipping 'Share' step to avoid quota issues.")
        elif boss_email and not client_config.get("is_shared"):
             logging.info(f"Sharing sheet with {boss_email}...")
             sheets.share_sheet(boss_email)
             # Update Firestore to mark as shared
             project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
             fs_config = FirestoreConfig(project_id) 
             fs_config.update_client_shared_status(client_id, True)
             
    except Exception as e:
        logging.error(f"[FATAL] Google Sheets Init Error for {client_name}: {e}")
        return

    # 4. Init KSeF Client
    base_url = get_base_url(ksef_env)
    ksef = KsefClient(base_url, nip, ksef_token)
    
    try:
        ksef.authenticate()
    except Exception as e:
        logging.error(f"[FATAL] KSeF Auth Error for {client_name}: {e}")
        return

    # 5. Determine Date Range
    # Cloud Run implies Daily Sync (or based on execution_mode)
    # Default to "Last 24h" logic for automation
    
    end_date = datetime.datetime.now()
    # Simple logic: sync last 2 days to be safe for overlap? Or just 1 day.
    # User requested: "Mode C (Daily) or Mode E (Incremental)"
    # Let's do Last 3 Days to catch weekends/delays safely, deduplication handles the rest.
    start_date = end_date - datetime.timedelta(days=3) 
    
    logging.info(f"Fetching invoices from {start_date} to {end_date}...")
    
    start_iso = start_date.astimezone().isoformat()
    end_iso = end_date.astimezone().isoformat()

    try:
        invoices = ksef.get_invoices(start_iso, end_iso)
    except Exception as e:
        logging.error(f"Error fetching invoices for {client_name}: {e}")
        return

    if not invoices:
        logging.info("No new invoices found.")
        return

    # 6. Format Data
    formatted_rows = []
    
    for inv in invoices:
        ksef_id = inv.get("ksefNumber") or inv.get("ksefReferenceNumber") or inv.get("referenceNumber")
        if not ksef_id:
             ksef_id = f"NO_ID_{datetime.datetime.now().timestamp()}_{inv.get('invoicingDate','')}"

        nr_dok = inv.get("invoiceNumber") or inv.get("invoiceReferenceNumber", "")
        
        # Seller
        seller_name = "Nieznany"
        if "seller" in inv and isinstance(inv["seller"], dict):
            seller_name = inv["seller"].get("name", "Nieznany")
        elif "subjectBy" in inv:
            sb = inv["subjectBy"]
            if "issuedByName" in sb: seller_name = sb["issuedByName"]
            elif "issuedByIdentifier" in sb: 
                ident = sb["issuedByIdentifier"]
                seller_name = ident.get("identifier", "Nieznany ID") if isinstance(ident, dict) else str(ident)

        # Dates
        invoicing_date = inv.get("issueDate") or inv.get("invoicingDate", "")
        if invoicing_date:
            invoicing_date = invoicing_date[:10]
        acquisition_date = ""  
        
        # Amounts
        netto = inv.get("netAmount")
        if netto is None: netto = inv.get("net", 0.0)
        
        brutto = inv.get("grossAmount")
        if brutto is None: brutto = inv.get("gross", 0.0)
        
        try:
            netto = float(netto)
        except:
            netto = 0.0
            
        try:
            brutto = float(brutto)
        except:
            brutto = 0.0
        
        row = [
            ksef_id,
            seller_name,
            nr_dok,
            invoicing_date,
            acquisition_date, 
            netto,
            brutto
        ]
        formatted_rows.append(row)

    # 7. Sync to Sheets
    logging.info(f"Syncing {len(formatted_rows)} invoices to Sheets...")
    sheets.sync_formatted_data(formatted_rows)
    logging.info(f"Success for {client_name}.")


def run_cloud_mode():
    logging.info("Starting KSeF Sync in CLOUD mode.")
    
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        logging.warning("GOOGLE_CLOUD_PROJECT env var not set. Attempting to run without explicitly setting it (libs might infer).")

    # 1. Setup Secrets
    secrets_mgr = SecretsManager(project_id)
    
    # Get Google Sheets Service Account JSON
    # Secret Name: google-sheets-creds
    logging.info("Fetching Google Sheets Credentials from Secret Manager...")
    sheets_creds_json = secrets_mgr.get_secret("google-sheets-creds")
    
    if not sheets_creds_json:
        logging.critical("CRITICAL: Could not fetch 'google-sheets-creds'. Aborting.")
        sys.exit(1)
        
    try:
        sheets_creds = json.loads(sheets_creds_json)
    except json.JSONDecodeError as e:
        logging.critical(f"CRITICAL: 'google-sheets-creds' is not valid JSON: {e}")
        sys.exit(1)

    # 2. Fetch Clients
    logging.info("Fetching Client Configuration from Firestore...")
    fs = FirestoreConfig(project_id)
    clients = fs.get_active_clients()
    
    if not clients:
        logging.warning("No active clients found in Firestore.")
        return

    logging.info(f"Found {len(clients)} clients.")

    # 3. Loop Clients
    # Get Global KSeF Env from env var or default to PROD for Cloud
    ksef_env = os.getenv("KSEF_ENV", "PROD")
    
    for client in clients:
        try:
            process_single_client(client, secrets_mgr, sheets_creds, ksef_env)
        except Exception as e:
            logging.error(f"Unhandled error processing client {client.get('client_name')}: {e}")

    logging.info("Cloud Run Execution Complete.")

def run_local_legacy_mode():
    """
    Preserved for backward compatibility or local testing with .env if used.
    """
    print("Executing in LOCAL LEGACY mode (not implemented in this refactor, please use Cloud Mode logic or adapt).")
    print("For local testing of Cloud Logic, set EXECUTION_ENV=CLOUD and ensure you have GCP credentials.")

def main():
    execution_env = os.getenv("EXECUTION_ENV", "LOCAL")
    
    if execution_env == "CLOUD":
        run_cloud_mode()
    else:
        # Check if user wants to force Cloud Logic locally
        parser = argparse.ArgumentParser()
        parser.add_argument("--cloud-sim", action="store_true", help="Simulate Cloud Run locally")
        args, unknown = parser.parse_known_args()
        
        if args.cloud_sim:
            # You need local Application Default Credentials for this to work
            os.environ["EXECUTION_ENV"] = "CLOUD"
            run_cloud_mode()
        else:
            run_local_legacy_mode()

if __name__ == "__main__":
    main()

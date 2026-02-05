import os
import sys
from datetime import datetime, date
from dotenv import load_dotenv
from ksef_client import KsefClient
from sheets_client import SheetsClient

# Load env variables
load_dotenv()

KSEF_URL = "https://api-test.ksef.mf.gov.pl/v2/"
KSEF_NIP = os.getenv("KSEF_NIP")
KSEF_TOKEN = os.getenv("KSEF_TOKEN")
SHEET_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "KSeF Invoices Sync")

def main():
    print("--- KSeF Invoice Sync Started ---")
    
    # Validation
    if not KSEF_NIP or not KSEF_TOKEN:
        print("Error: KSEF_NIP or KSEF_TOKEN missing in .env")
        sys.exit(1)
        
    if not os.path.exists(SHEET_CREDENTIALS):
        print(f"Error: {SHEET_CREDENTIALS} not found. Please setup Google Cloud Credentials.")
        sys.exit(1)

    # 1. Initialize Clients
    ksef = KsefClient(KSEF_URL, KSEF_NIP, KSEF_TOKEN)
    sheets = SheetsClient(SHEET_CREDENTIALS, SHEET_NAME)

    # 2. Authenticate Sheets & Get Existing Data
    try:
        sheets.authenticate()
        sheets.get_or_create_sheet()
        existing_ids = sheets.get_existing_ids()
        print(f"Found {len(existing_ids)} existing invoices in Sheet.")
    except Exception as e:
        print(f"Google Sheets Error: {e}")
        sys.exit(1)

    # 3. Authenticate KSeF
    try:
        ksef.authenticate()
    except Exception as e:
        print(f"KSeF Auth Error: {e}")
        sys.exit(1)

    # 4. Fetch Invoices from Start of Month
    # Format: YYYY-MM-DDT00:00:00+01:00 (ISO 8601)
    today = date.today()
    start_of_month = datetime(today.year, today.month, 1).strftime("%Y-%m-%dT00:00:00+00:00")
    
    try:
        invoices = ksef.get_invoices(start_of_month)
    except Exception as e:
        print(f"KSeF Query Error: {e}")
        sys.exit(1)

    # 5. Process Invoices
    new_rows = []
    skipped_count = 0
    
    for invoice in invoices:
        # Correct keys based on actual JSON response
        ksef_id = invoice.get('ksefNumber')
        
        if ksef_id in existing_ids:
            skipped_count += 1
            continue
            
        # Parse Details (Revised for Subject1/Sales or Subject2/Purchase common structure)
        # Structure seen in logs:
        # { 'ksefNumber': '...', 'invoiceNumber': '...', 'seller': {'nip': '...', 'name': '...'}, ... }
        
        # Seller
        seller_data = invoice.get('seller', {})
        seller_nip = seller_data.get('nip', 'N/A')
        seller_name = seller_data.get('name', 'Unknown')
        
        # Invoice Number
        invoice_number = invoice.get('invoiceNumber', 'N/A')
        
        # Issue Date
        # Prefer 'invoicingDate' if available, else 'issueDate' (YYYY-MM-DD vs ISO)
        issue_date = invoice.get('invoicingDate', invoice.get('issueDate', 'N/A'))
        
        # Amounts
        net_amount = invoice.get('netAmount', 0.0)
        gross_amount = invoice.get('grossAmount', 0.0)
        currency = invoice.get('currency', 'PLN')
        
        row = [
            ksef_id,
            invoice_number,
            seller_nip,
            seller_name,
            issue_date,
            net_amount,
            gross_amount,
            currency
        ]
        new_rows.append(row)

    # 6. Sync to Sheets
    if new_rows:
        print(f"Syncing {len(new_rows)} new invoices...")
        try:
            # Use new formatted sync
            sheets.sync_formatted_data(new_rows)
            # print("Sync Complete!") # handled in method
        except Exception as e:
            print(f"Error syncing to sheet: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("No new invoices to sync.")
        # Optional: Force re-format even if no new data?
        # User might want to re-organize existing data.
        # Let's add a flag or just do it once if requested.
        # For now, only on new data or if we force it.
        # Actually, let's force re-format to apply the new layout to existing data immediately.
        try:
             print("Applying new layout to existing data...")
             sheets.sync_formatted_data([])
        except Exception as e:
             print(f"Error re-formatting: {e}")
     # No duplicate else here
    print(f"Summary: Found {len(invoices)} total, {skipped_count} duplicates, {len(new_rows)} synced.")

if __name__ == "__main__":
    main()

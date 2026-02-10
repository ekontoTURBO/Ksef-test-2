import os
import sys
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from ksef_client import KsefClient
from sheets_client import SheetsClient

# Load env variables
load_dotenv()

KSEF_URL = "https://api-test.ksef.mf.gov.pl/v2/"
KSEF_NIP = os.getenv("KSEF_NIP")
KSEF_TOKEN = os.getenv("KSEF_TOKEN")
import os
import sys
from datetime import datetime, date, timedelta
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
PROGRESS_FILE = "progress.txt"

def save_progress(date_iso):
    """Saves the last successfully synced date (upper bound of next chunk)."""
    with open(PROGRESS_FILE, "w") as f:
        f.write(date_iso)

def load_progress():
    """Loads the last successfully synced date."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return f.read().strip()
    return None

def fetch_invoices_reverse_chunks(ksef, start_date, end_date, chunk_days=30):
    """
    Fetches invoices in chunks of `chunk_days` working BACKWARDS from end_date to start_date.
    Saves progress after each chunk.
    """
    all_invoices = []
    current_end = end_date
    
    # We loop until current_end is back at (or before) start_date
    while current_end > start_date:
        current_start = current_end - timedelta(days=chunk_days)
        if current_start < start_date:
            current_start = start_date
            
        start_iso = current_start.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        end_iso = current_end.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        
        print(f"\n--- Chunk (Reverse): {start_iso} to {end_iso} ---")
        try:
            batch = ksef.get_invoices(start_iso, end_iso, page_size=1000)
            all_invoices.extend(batch)
            
            # Save checkpoint: We successfully synced down to 'current_start'.
            # Next run should pick up from 'current_start'.
            save_progress(start_iso)
            
        except Exception as e:
            print(f"ERROR fetching chunk {start_iso} - {end_iso}: {e}")
            print("Stopping sync to preserve checkpoint integrity.")
            # We raise so main stops processing and doesn't mark things done that failed
            raise e
            
        # Move back
        # current_end becomes current_start (minus 1 sec if strict, but ranges usually fine)
        # To match the "1 sec safety" from forward loop, we subtract 1 sec from new end.
        current_end = current_start - timedelta(seconds=1)
        
    return all_invoices

def main():
    print("--- KSeF Robust Triple-Mode Sync Started ---")
    
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

    # 2. Interactive Mode Selection
    print("\nSelect Sync Mode:")
    print("A: Complete History (Reverse 30-day Chunks + Checkpointing)")
    print("B: Quarterly Catch-up (Last 90 Days)")
    print("C: Weekly Pulse (Last 7 Days)")
    
    try:
        choice = input("Enter choice (A/B/C): ").strip().upper()
    except EOFError:
        print("Input error, defaulting to C (Weekly).")
        choice = 'C'

    if choice not in ['A', 'B', 'C']:
        print("Invalid choice. Exiting.")
        sys.exit(1)

    # 3. Authenticate Sheets (Authenticate KSeF later to save session time if Resume prompt waits)
    try:
        sheets.authenticate()
        sheets.get_or_create_sheet()
    except Exception as e:
        print(f"Google Sheets Error: {e}")
        sys.exit(1)
        
    # 4. Configure Sync Parameters
    today = datetime.now()
    existing_ids = set()
    invoices = []
    
    if choice == 'A':
        print("\n--- MODE A: COMPLETE HISTORY (REVERSE) ---")
        # Default Full History Limit
        absolute_start = datetime(2022, 1, 1)
        
        # Check Checkpoint
        last_checkpoint = load_progress()
        current_upper_bound = today
        
        should_resume = False
        if last_checkpoint:
            print(f"Checkpoint found: {last_checkpoint}")
            # Ask to resume? Or auto-resume? User said "If restarted, it should start from that date"
            # Interactive prompt implies we can ask.
            try:
                res = input(f"Resume fetching from {last_checkpoint}? (Y/N): ").strip().upper()
                if res == 'Y':
                    should_resume = True
                    # If resuming, our "End Date" for the loop is the checkpoint date
                    # We continue going BACKWARDS from there.
                    try:
                        # Parse checkpoint date (ISO)
                        # Handle potential timezone Z replacement if needed
                        if 'Z' in last_checkpoint:
                             current_upper_bound = datetime.fromisoformat(last_checkpoint.replace('Z', '+00:00'))
                        else:
                             current_upper_bound = datetime.fromisoformat(last_checkpoint)
                    except ValueError:
                        print("Invalid checkpoint format. Starting fresh.")
                        should_resume = False
            except EOFError:
                pass
        
        if not should_resume:
            print("Starting fresh sync from Today backwards to 2022...")
            # If fresh, clear sheet? User said "Clear the Google Sheet" for Mode A.
            # But if resuming, we SHOULD NOT clear.
            sheets.clear_sheet()
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)
            current_upper_bound = today
            
        print("Fetching existing ids (just in case)...")
        # existing_ids = sheets.get_existing_ids() # If clear_sheet was called, this is empty.
        # If resuming, we need them to avoid dupes at the boundary?
        # Actually dedupe logic handles it. Mode A chunks might overlap slightly if not careful, 
        # but dedupe protects.
        if should_resume:
             existing_ids = sheets.get_existing_ids()
        
        # 5. Authenticate KSeF
        try:
            ksef.authenticate()
        except Exception as e:
            print(f"KSeF Auth Error: {e}")
            sys.exit(1)
            
        # 6. Fetch (Reverse)
        print(f"Fetching reversed chunks from {current_upper_bound} down to {absolute_start}...")
        try:
            invoices = fetch_invoices_reverse_chunks(ksef, absolute_start, current_upper_bound, chunk_days=30)
        except Exception as e:
            print(f"\nSync Interrupted: {e}")
            print("Progress saved. Restart script to resume.")
            # We still want to save what we have? 
            # fetch handles batch extension, so invoices list has the success batches.
            # We can process what we have.
            pass

    elif choice == 'B':
        print("\n--- MODE B: QUARTERLY CATCH-UP ---")
        start_date = today - timedelta(days=90)
        start_date_iso = start_date.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        end_date_iso = today.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        
        print("Fetching existing invoices for deduplication...")
        existing_ids = sheets.get_existing_ids()
        print(f"Found {len(existing_ids)} existing ids.")
        
        # Authenticate
        try:
            ksef.authenticate()
        except Exception: 
            sys.exit(1)
            
        print(f"Fetching from {start_date_iso} to {end_date_iso}...")
        try:
            # 10k Limit in Mode B check
            # ksef_client handles the safe stop.
            invoices = ksef.get_invoices(start_date_iso, end_date_iso, page_size=1000)
        except Exception as e:
            print(f"KSeF Query Error: {e}")
            sys.exit(1)
        
    elif choice == 'C':
        print("\n--- MODE C: WEEKLY PULSE ---")
        start_date = today - timedelta(days=7)
        start_date_iso = start_date.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        end_date_iso = today.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        
        print("Fetching existing invoices for deduplication...")
        existing_ids = sheets.get_existing_ids()
        print(f"Found {len(existing_ids)} existing ids.")
        
        # Authenticate
        try:
            ksef.authenticate()
        except Exception: 
            sys.exit(1)
            
        print(f"Fetching from {start_date_iso} to {end_date_iso}...")
        try:
            invoices = ksef.get_invoices(start_date_iso, end_date_iso, page_size=1000)
        except Exception as e:
            print(f"KSeF Query Error: {e}")
            sys.exit(1)

    # 7. Process & Deduplicate
    new_rows = []
    skipped_count = 0
    
    print(f"\nProcessing {len(invoices)} retrieved invoices...")
    for invoice in invoices:
        # Field Mapping
        # ksefReferenceNumber -> Col A
        ksef_id = invoice.get('ksefReferenceNumber') or invoice.get('ksefNumber')
        
        if not ksef_id:
             continue
             
        if ksef_id in existing_ids:
            skipped_count += 1
            continue
            
        # Parse Subject2 Fields
        # invoiceNumber -> Col B
        inv_num = invoice.get('invoiceReferenceNumber') or invoice.get('invoiceNumber') or 'N/A'
        
        # Seller NIP -> Col C
        seller = invoice.get('seller', {})
        seller_nip = seller.get('nip') or invoice.get('issuedBy', {}).get('identifier', {}).get('identifier') or 'N/A'
        
        # Seller Name -> Col D
        seller_name = seller.get('name') \
                      or invoice.get('issuedBy', {}).get('name', {}).get('tradeName') \
                      or invoice.get('issuedBy', {}).get('name', {}).get('fullName') \
                      or 'Unknown'
                      
        # Invoicing Date -> Col E
        inv_date = invoice.get('invoicingDate') or invoice.get('acquisitionDate') or 'N/A'
        if inv_date and 'T' in inv_date:
            inv_date = inv_date.split('T')[0]
            
        # Amounts -> Col F, G
        net = invoice.get('netAmount', 0.0)
        gross = invoice.get('grossAmount', 0.0)
        
        # Currency -> Col H
        curr = invoice.get('currency', 'PLN')
        
        row = [ksef_id, inv_num, seller_nip, seller_name, inv_date, net, gross, curr]
        new_rows.append(row)
        existing_ids.add(ksef_id) # Prevent dupes in same batch

    # 8. Sync to Sheets
    if new_rows:
        print(f"Syncing {len(new_rows)} new invoices...")
        try:
            sheets.sync_formatted_data(new_rows)
        except Exception as e:
            print(f"Error syncing to sheet: {e}")
    else:
        print("No new invoices to sync.")
        
    print(f"\nSummary:")
    print(f"Total Fetched: {len(invoices)}")
    print(f"Duplicates Skipped: {skipped_count}")
    print(f"New Synced: {len(new_rows)}")
    print("--- Sync Complete ---")

if __name__ == "__main__":
    main()

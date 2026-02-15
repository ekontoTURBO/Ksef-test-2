import os
import sys
import json
import time
import argparse
import datetime
from dotenv import load_dotenv

# Import Clients
from ksef_client import KsefClient
from sheets_client import SheetsClient

# --- Constants & Config ---
METADATA_FILE = "client_metadata.json"
TOKEN_FILE = "token.json"
ENV_FILE = ".env"

def load_secrets():
    """
    Loads secrets from .env with strict sanitization.
    Returns a dict of cleaned secrets.
    """
    load_dotenv()
    
    def get_clean(key, required=True):
        val = os.getenv(key)
        if val:
            # Remove quotes and whitespace
            val = val.strip().strip("'").strip('"')
        
        if required and not val:
            print(f"[CRITICAL] Missing required env var: {key}")
            sys.exit(1)
        return val

    return {
        "KSEF_ENV": get_clean("KSEF_ENV"),
        "KSEF_NIP": get_clean("KSEF_NIP"),
        "KSEF_TOKEN": get_clean("KSEF_TOKEN"),
        "GOOGLE_CREDENTIALS_PATH": get_clean("GOOGLE_CREDENTIALS_PATH", required=False) or "credentials.json" 
    }

def get_base_url(env):
    env = env.upper()
    if env == "PROD":
        # Production API V2
        return "https://api.ksef.mf.gov.pl/api/v2"
    elif env == "DEMO":
        return "https://ksef-demo.mf.gov.pl/api/online"
    else: # TEST
        return "https://ksef-test.mf.gov.pl/api/online"

def first_run_setup():
    """
    Wizard for first-time setup. Creates client_metadata.json.
    """
    print("\n--- PIERWSZE URUCHOMIENIE (SETUP) ---")
    print("Wygląda na to, że konfiguracja klienta nie istnieje.")
    
    client_name = input("Co to za klient? (Nazwa): ").strip()
    sheet_name = input("Jak nazwać arkusz Google Sheets?: ").strip()
    boss_email = input("Email szefa do udostępnienia arkusza?: ").strip()
    frequency = input("Częstotliwość (Dzień/Tydzień/Godzina)?: ").strip()
    
    metadata = {
        "client_name": client_name,
        "sheet_name": sheet_name,
        "boss_email": boss_email,
        "frequency": frequency,
        "last_sync": None
    }
    
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)
        
    print(f"Zapisano konfigurację w {METADATA_FILE}.\n")
    
    # Try to share immediately if possible
    try:
        secrets = load_secrets()
        creds_path = secrets["GOOGLE_CREDENTIALS_PATH"]
        if boss_email:
             sheets = SheetsClient(creds_path, sheet_name)
             sheets.authenticate()
             sheets.get_or_create_sheet()
             sheets.share_sheet(boss_email)
    except Exception as e:
        print(f"Warning: Could not share sheet during setup: {e}")
        
    return metadata

def load_metadata():
    if not os.path.exists(METADATA_FILE):
        return None
    with open(METADATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def update_last_sync(metadata):
    metadata["last_sync"] = datetime.datetime.now().isoformat()
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)

def perform_sync(secrets, metadata, auto_mode=False):
    """
    Main Sync Logic.
    """
    print(f"\n--- Rozpoczynanie synchronizacji dla: {metadata['client_name']} ---")
    
    # 1. KSeF Init
    base_url = get_base_url(secrets["KSEF_ENV"])
    nip = secrets["KSEF_NIP"]
    token = secrets["KSEF_TOKEN"]
    
    ksef = KsefClient(base_url, nip, token)
    
    try:
        ksef.authenticate()
    except Exception as e:
        print(f"[FATAL] Błąd autoryzacji KSeF: {e}")
        return

    # 2. Sheets Init
    sheet_name = metadata["sheet_name"]
    creds_path = secrets["GOOGLE_CREDENTIALS_PATH"]
    
    sheets = SheetsClient(creds_path, sheet_name)
    try:
        sheets.authenticate()
        sheets.get_or_create_sheet()
        
        # Share with Boss logic MOVED to First Run / Setup
        # Only check if specifically requested or maybe just rely on setup.
        # To be safe for existing users who might have missed it, we could add a flag, 
        # but user specifically asked to "Stop the Email Spam" so we remove it from here.
            
    except Exception as e:
        print(f"[FATAL] Błąd Google Sheets: {e}")
        return

    # 3. Determine Date Range
    # If Auto Mode -> Last 24h
    # If Manual -> Ask or Smart Sync? 
    # For now, let's implement the Auto logic as default for logic simplicity in this overhaul,
    # or implement a simple choice if not auto.
    
    end_date = datetime.datetime.now()
    
    if auto_mode:
        print("[AUTO] Pobieranie faktur z ostatnich 24 godzin...")
        start_date = end_date - datetime.timedelta(days=1)
    else:
        # Manual Mode Choice
        print("\nWybierz zakres dat:")
        print("1. Ostatnie 24h")
        print("2. Ostatnie 7 dni")
        print("3. Ostatnie 30 dni")
        print("4. Ostatnie 90 dni")
        print("5. Wszystko (Initial Load - Może trwać długo)")
        
        choice = input("Wybór [1]: ").strip()
        
        if choice == "2":
            start_date = end_date - datetime.timedelta(days=7)
        elif choice == "3":
            start_date = end_date - datetime.timedelta(days=30)
        elif choice == "4":
            start_date = end_date - datetime.timedelta(days=90)
        elif choice == "5":
            # Arbitrary old date
            start_date = datetime.datetime(2022, 1, 1)
        else:
            start_date = end_date - datetime.timedelta(days=1)
    
    # Format to ISO 8601
    # KSeF expects: YYYY-MM-DDThh:mm:ss+00:00 (or Z)
    start_iso = start_date.astimezone().isoformat()
    end_iso = end_date.astimezone().isoformat()
    
    # 4. Fetch from KSeF
    try:
        # Assuming get_invoices handles the actual fetching loop
        invoices = ksef.get_invoices(start_iso, end_iso)
    except Exception as e:
        print(f"Błąd pobierania faktur: {e}")
        return

    if not invoices:
        print("Brak nowych faktur w zadanym okresie.")
        return

    # SheetsClient.sync_formatted_data expects a list of rows.
    # We need to map KSeF Invoice objects to rows matching our Headers.
    # Headers: KSeF ID, Sprzedawca, Nr dokumentu, Data, TERMIN, Netto, Brutto, Kategoria, PŁATNOŚĆ, LOKAL, UWAGI
    
    if len(invoices) > 0:
        print("\n[DEBUG] Przykładowa faktura (struktura):")
        # Print first invoice keys to debug data loss
        # print(list(invoices[0].keys()))
        # print(json.dumps(invoices[0], indent=2, ensure_ascii=False)[:500] + "...")
        
        # DUMP TO FILE FOR AGENT INSPECTION
        with open("debug_invoice.json", "w", encoding="utf-8") as f:
            json.dump(invoices[0], f, indent=4, ensure_ascii=False)
        print("[DEBUG] Zapisano 'debug_invoice.json'.")
    
    formatted_rows = []
    
    for inv in invoices:
        # Mapping logic based on debug_invoice.json
        # Structure:
        # "ksefNumber": "...",
        # "invoiceNumber": "...",
        # "seller": { "name": "..." },
        # "issueDate": "...",
        # "netAmount": 82.9,
        # "grossAmount": 87.05
        
        ksef_id = inv.get("ksefNumber") or inv.get("ksefReferenceNumber") or inv.get("referenceNumber")
        
        # Fallback ID
        if not ksef_id:
             ksef_id = f"NO_ID_{datetime.datetime.now().timestamp()}_{inv.get('invoicingDate','')}"

        nr_dok = inv.get("invoiceNumber") or inv.get("invoiceReferenceNumber", "")
        
        # Seller
        seller_name = "Nieznany"
        if "seller" in inv and isinstance(inv["seller"], dict):
            seller_name = inv["seller"].get("name", "Nieznany")
        elif "subjectBy" in inv:
            # Fallback to old logic just in case
            sb = inv["subjectBy"]
            if "issuedByName" in sb: seller_name = sb["issuedByName"]
            elif "issuedByIdentifier" in sb: 
                ident = sb["issuedByIdentifier"]
                seller_name = ident.get("identifier", "Nieznany ID") if isinstance(ident, dict) else str(ident)

        # Dates
        invoicing_date = inv.get("issueDate") or inv.get("invoicingDate", "")
        if invoicing_date:
            invoicing_date = invoicing_date[:10]
        acquisition_date = ""  # Force empty TERMIN 
        
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
            acquisition_date, # Placeholder for TERMIN
            netto,
            brutto
        ]
        
        formatted_rows.append(row)
        
    print(f"Przetwarzanie {len(formatted_rows)} faktur do arkusza...")
    sheets.sync_formatted_data(formatted_rows)
    
    update_last_sync(metadata)
    print("Sukces!")

def reset_application():
    """
    Deletes config files and restarts.
    """
    print("\n[RESET] Usuwanie plików konfiguracyjnych...")
    
    files_to_remove = [METADATA_FILE, TOKEN_FILE]
    for f in files_to_remove:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"Usunięto: {f}")
            except Exception as e:
                print(f"Błąd usuwania {f}: {e}")
                
    print("Restartowanie aplikacji...")
    time.sleep(1)
    # Restart the script
    python = sys.executable
    os.execl(python, python, *sys.argv)

def main():
    parser = argparse.ArgumentParser(description="KSeF Sync Tool")
    parser.add_argument("--auto", action="store_true", help="Run in autonomous cloud mode (daily sync, no menu)")
    args = parser.parse_args()
    
    # 1. Secrets
    secrets = load_secrets()
    
    # 2. Metadata / First Run
    metadata = load_metadata()
    
    if args.auto:
        if not metadata:
            print("[AUTO] ERROR: setup not completed (client_metadata.json missing). Run manually first.")
            sys.exit(1)
            
        perform_sync(secrets, metadata, auto_mode=True)
        sys.exit(0)
        
    # Manual Mode
    if not metadata:
        metadata = first_run_setup()
        
    while True:
        print(f"\n=== MENU GŁÓWNE: {metadata['client_name']} ===")
        print("[1] Synchronizuj (Sync)")
        print("[R] Resetuj ustawienia (Reset)")
        print("[E] Wyjście")
        
        choice = input("Wybór: ").upper().strip()
        
        if choice == "1":
            perform_sync(secrets, metadata)
        elif choice == "R":
            confirm = input("Czy na pewno chcesz usunąć dane i zresetować? (T/N): ").upper()
            if confirm == "T":
                reset_application()
        elif choice == "E":
            print("Do widzenia.")
            sys.exit(0)
        else:
            print("Nieznana opcja.")

if __name__ == "__main__":
    main()

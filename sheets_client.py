import os.path
import datetime
from collections import defaultdict
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build # Added for Drive API

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

class SheetsClient:
    def __init__(self, credentials_path, sheet_name):
        self.credentials_path = credentials_path
        self.sheet_name = sheet_name
        self.creds = None
        self.client = None
        self.spreadsheet = None 
        self.sheet = None
        
        self.month_names = {
            1: "STYCZEŃ", 2: "LUTY", 3: "MARZEC", 4: "KWIECIEŃ", 5: "MAJ", 6: "CZERWIEC", 
            7: "LIPIEC", 8: "SIERPIEŃ", 9: "WRZESIEŃ", 10: "PAŹDZIERNIK", 11: "LISTOPAD", 12: "GRUDZIEŃ"
        }

    def authenticate(self):
        """Authenticates with Google and creates a gspread client."""
        # Check existing token
        if os.path.exists("token.json"):
            try:
                self.creds = Credentials.from_authorized_user_file("token.json", SCOPES)
            except Exception:
                print("Corrupted token.json found. Deleting...")
                os.remove("token.json")
                self.creds = None
        
        # If valid credentials exist, try to use/refresh them
        if self.creds and self.creds.expired and self.creds.refresh_token:
            try:
                self.creds.refresh(Request())
            except RefreshError:
                print("Token expired and refresh failed (invalid_grant). Deleting token.json to re-authenticate...")
                if os.path.exists("token.json"):
                    os.remove("token.json")
                self.creds = None
            except Exception as e:
                print(f"Error refreshing token: {e}. Deleting token.json...")
                if os.path.exists("token.json"):
                    os.remove("token.json")
                self.creds = None

        # If no valid creds (either didn't exist, expired & failed refresh, or deleted), do full login
        if not self.creds or not self.creds.valid:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Missing {self.credentials_path}.")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                self.credentials_path, SCOPES
            )
            # FORCE offline access to get a refresh_token
            # FORCE consent prompt to ensure we get a refresh_token even if user previously approved
            self.creds = flow.run_local_server(port=8080, access_type='offline', prompt='consent')
            
            with open("token.json", "w") as token:
                token.write(self.creds.to_json())

        self.client = gspread.authorize(self.creds)
        print("Authenticated with Google Sheets.")

    def share_sheet(self, email):
        """Shares the spreadsheet with the specified email using Drive API."""
        if not self.spreadsheet:
             print("Spreadsheet not open, cannot share.")
             return

        try:
            drive_service = build('drive', 'v3', credentials=self.creds)
            
            # Check if already shared? API allows redundancy, usually fine.
            # We want 'writer' role.
            permission = {
                'type': 'user',
                'role': 'writer',
                'emailAddress': email
            }
            
            print(f"Sharing sheet '{self.sheet_name}' with {email}...")
            drive_service.permissions().create(
                fileId=self.spreadsheet.id,
                body=permission,
                fields='id'
            ).execute()
            print("Shared successfully.")
            
        except Exception as e:
            print(f"Error sharing sheet: {e}")

    def get_or_create_sheet(self):
        """Opens the sheet or creates it."""
        try:
            self.spreadsheet = self.client.open(self.sheet_name)
            self.sheet = self.spreadsheet.sheet1
            print(f"Opened existing sheet: {self.sheet_name}")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Sheet '{self.sheet_name}' not found. Creating...")
            self.spreadsheet = self.client.create(self.sheet_name)
            self.sheet = self.spreadsheet.sheet1
            # Add Headers immediately
            headers = [
                "KSeF ID", "Sprzedawca", "Nr dokumentu", "Data", "TERMIN", 
                "Netto", "Brutto", "Kategoria", "PŁATNOŚĆ", "LOKAL", "UWAGI"
            ]
            self.sheet.append_row(headers)
            print("Created new sheet with headers.")

    def clear_sheet(self):
        """Clears the sheet."""
        print("Clearing sheet...")
        self.sheet.clear()
        headers = [
            "KSeF ID", "Sprzedawca", "Nr dokumentu", "Data", "TERMIN", 
            "Netto", "Brutto", "Kategoria", "PŁATNOŚĆ", "LOKAL", "UWAGI"
        ]
        self.sheet.append_row(headers)

    def get_existing_ids(self):
        """Fetches all KSeF IDs from Column A."""
        try:
            ids = self.sheet.col_values(1)
            if ids and ids[0] == "KSeF ID":
                return set(ids[1:])
            return set(ids)
        except Exception as e:
            print(f"Warning reading existing IDs: {e}")
            return set()

    def sync_formatted_data(self, new_rows):
        """
        Main Overhaul Logic:
        1. Read ALL data (preserve manual cols).
        2. Merge with NEW data.
        3. Sort by Date Ascending.
        4. Group by Month (with Headers).
        5. Rewrite Sheet & Apply Formatting.
        """
        print("--- syncing formatted data ---")
        
        # --- Step 1: Read Existing Data & Map Manual Columns ---
        all_values = self.sheet.get_all_values()
        existing_data_map = {} # Map KSeF ID -> [Manual Cols Data]
        
        # Headers are: KSeF ID (0), ..., Netto(5), Brutto(6), Kategoria(7), PŁATNOŚĆ(8), LOKAL(9), UWAGI(10)
        # We need to preserve cols 7, 8, 9, 10 (Indices 7-10)
        
        if all_values:
            # Skip header if present
            start_idx = 1 if (all_values and all_values[0] and all_values[0][0] == "KSeF ID") else 0
            
            for row in all_values[start_idx:]:
                if not row or not row[0]: continue # Skip empty
                
                k_id = row[0]
                # Extract manual cols. logic: ensure row has enough cols
                manual_notes = ["", "", "", ""] # Default empty for 4 cols
                
                # Slices for manual cols (7 to 11)
                if len(row) > 7:
                    manual_segment = row[7:11]
                    # Fill logic
                    for i in range(len(manual_segment)):
                        manual_notes[i] = manual_segment[i]
                
                existing_data_map[k_id] = manual_notes

        # --- Step 2: Merge Data ---
        # new_rows structure: [ID, Sprzedawca, Nr, Data, Termin, Net, Gross] (Length 7)
        # We need to construct full rows: [Basic 7] + [Manual 4]
        
        merged_rows = []
        processed_ids = set()
        
        # A. Add New Rows (and update if they existed, keeping notes)
        for r in new_rows:
            k_id = r[0]
            manual = existing_data_map.get(k_id, ["", "", "", ""])
            full_row = r + manual
            merged_rows.append(full_row)
            processed_ids.add(k_id)
            
        # B. Add 'Only Existing' Rows (that were not in new_rows, but are in sheet)
        # We need to reconstruct their basic data too? 
        # Wait. 'new_rows' only contains *fetched* rows.
        # If we are doing 'Incremental Sync', we might miss old rows if we don't read them back.
        # Current logic in main.py fetches *duplicates* but potentially filters them out before calling this?
        # NO. main.py 'new_rows' ONLY contains rows that were NOT in 'existing_ids'.
        # So 'new_rows' are purely NEW.
        
        # The 'existing_data_map' only stored manual notes. We need the FULL existing row data!
        # Let's adjust Step 1.
        
        existing_full_rows = []
        if all_values:
             start_idx = 1 if (all_values and all_values[0] and all_values[0][0] == "KSeF ID") else 0
             for row in all_values[start_idx:]:
                 if not row or not row[0]: continue
                 # Filter out Month Header rows (e.g. "--- STYCZEŃ ---")
                 if row[0].startswith("---"): continue
                 if "Total Net" in row[0] or (len(row) > 5 and "Total Net" in str(row[5])): continue # Summary rows? 
                 # Actually summary rows have empty first col usually?
                 if not row[0].strip(): continue
                 
                 existing_full_rows.append(row)
                 
        # Now Merge:
        # Start with Existing Rows...
        final_dataset = []
        existing_ids_set = set()
        
        for row in existing_full_rows:
            final_dataset.append(row)
            if row[0]: existing_ids_set.add(row[0])
            
        # Add New Rows
        for row in new_rows:
            k_id = row[0]
            if k_id not in existing_ids_set:
                 # Standardize length to 11
                 while len(row) < 11:
                     row.append("")
                 final_dataset.append(row)
                 existing_ids_set.add(k_id)

        # --- Step 3: Sort by Date Ascending ---
        def parse_date(d_str):
            if not d_str: return datetime.date.max
            try:
                return datetime.date.fromisoformat(d_str)
            except ValueError:
                try:
                    return datetime.datetime.fromisoformat(d_str).date()
                except ValueError:
                    return datetime.date.max # Push invalid to end
        
        # Date is column Index 3 (0-based: ID, Sprz, Nr, Data)
        final_dataset.sort(key=lambda x: parse_date(x[3]))

        # --- Step 4: Group & Construct Output ---
        output_rows = []
        
        # Headers
        headers = [
            "KSeF ID", "Sprzedawca", "Nr dokumentu", "Data", "TERMIN", 
            "Netto", "Brutto", "Kategoria", "PŁATNOŚĆ", "LOKAL", "UWAGI"
        ]
        output_rows.append(headers)
        
        row_groups = [] # List of {'start': int, 'length': int, 'depth': 1}
        # Actually API needs startIndex and endIndex.
        
        current_month_key = None
        group_start_index = None # 1-based index (Sheets API uses 0-based for specific calls, but usually 0-index)
        
        # The first data row is at index 1 (after header).
        current_write_idx = 1 
        
        today = datetime.date.today()
        current_real_month_key = (today.year, today.month)
        
        groups_metadata = [] # List of tuples (startIndex, endIndex, is_collapsed)
        
        # Helper to get month key
        def get_month_key(row):
            d = parse_date(row[3])
            if d == datetime.date.max: return None
            return (d.year, d.month)

        # We will iterate and insert Header Rows
        # But we need to separate data into blocks first to insert headers easily
        from itertools import groupby
        
        # Group by YearMonth
        # Note: groupby matches consecutive keys. Sort is active, so this works.
        grouped_data = groupby(final_dataset, key=get_month_key)
        
        for key, items in grouped_data:
            if not key: continue # Skip invalid dates (grouped at end)
            
            year, month = key
            month_items = list(items)
            
    # 1. Insert Month Header Row
            month_pl = self.month_names.get(month, "MIESIĄC")
            header_text = f"--- {month_pl} {year} ---"
            
            # Header Row - MUST be a list to occupy the row correctly
            output_rows.append([header_text])
            header_row_idx = current_write_idx
            current_write_idx += 1
            
            # 2. Add Items
            for item in month_items:
                output_rows.append(item)
            
            count = len(month_items)
            
            # 3. Define Group Range
            # Group should contain the ITEMS, but usually header is visible?
            # User logic: "Collapse previous months".
            # Usually you group the rows UNDER the header.
            # Start: header_row_idx + 1. End: header_row_idx + count.
            # OR include header? Likely want header visible, rows hidden.
            
            g_start = header_row_idx + 1
            g_end = header_row_idx + count # Inclusive for logic, but API might differ
            
            # API addDimensionGroup: range is startIndex (inclusive) to endIndex (exclusive)
            # We want to group the DATA rows.
            # So from g_start (row index) to g_start + count.
            
            is_current_month = (year == current_real_month_key[0] and month == current_real_month_key[1])
            collapsed = not is_current_month
            
            groups_metadata.append({
                "range": {
                    "sheetId": self.sheet.id,
                    "dimension": "ROWS",
                    "startIndex": g_start, 
                    "endIndex": g_start + count
                },
                "collapsed": collapsed
            })
            
            current_write_idx += count

        # --- Step 5: Write & Format ---
        print("Writing rewritten data to Sheet...")
        
        # Check size safe
        self.sheet.clear()
        
        try:
             self.sheet.update("A1", output_rows)
        except Exception:
             self.sheet.update(range_name="A1", values=output_rows)
             
        # Generate Batch Updates
        requests = []
        
        # 1. Remove all existing groups first?
        # To be safe, we should probably clear groups.
        requests.append({
            "deleteDimensionGroup": {
                "range": {
                    "sheetId": self.sheet.id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": len(output_rows) + 100
                }
            }
        })
        
        # 2. Add Groups
        # We need to add groups, then set their collapsed state.
        # Actually 'addDimensionGroup' doesn't set collapsed. 'updateDimensionGroup' does?
        # Or we rely on UI manual click? User asked: "collapsed/hide previous months"
        
        # We process groups in reverse order to avoid index shifts? No, groups don't shift indices.
        
        for g in groups_metadata:
            # Create Group
            requests.append({
                "addDimensionGroup": {
                    "range": g["range"]
                }
            })
            
            # Collapse/Expand (UpdateDimensionGroup functionality or specific Toggle)
            # Actually, to set collapsed state, use updateDimensionGroup
            # But we just created it.
            # Note: We can't consistently set 'collapsed' flag easily in one batch with creation 
            # without complex logic in some APIs.
            # However, standard practice: Create, then Update.
            
            if g["collapsed"]:
                 # We need to target the group we just made. 
                 # The 'range' identifies it.
                 requests.append({
                     "updateDimensionGroup": {
                         "dimensionGroup": {
                             "range": g["range"],
                             "depth": 1,
                             "collapsed": True
                         },
                         "fields": "collapsed"
                     }
                 })
                 
        # 3. Formatting
        # Header Row (Row 0): Green
        requests.append({
            "repeatCell": {
                "range": {"sheetId": self.sheet.id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.2, "green": 0.6, "blue": 0.3}, "textFormat": {"foregroundColor": {"red": 1,"green": 1,"blue": 1}, "bold": True}}},
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
            }
        })
        
        # Column Formatting
        # Netto (F/5), Brutto (G/6) -> Currency
        # Columns 5 and 6.
        requests.append({
            "repeatCell": {
                "range": {"sheetId": self.sheet.id, "startColumnIndex": 5, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00 zł"}}},
                "fields": "userEnteredFormat(numberFormat)"
            }
        })
        
        # Month Headers (Optional: Center, Bold, Light Green)
        # We need to find their indices again? Or just iterate output_rows
        # Iterating output_rows is safer.
        for idx, row in enumerate(output_rows):
            if row and len(row) > 0 and str(row[0]).startswith("---"):
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": self.sheet.id, "startRowIndex": idx, "endRowIndex": idx+1},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.9, "green": 0.95, "blue": 0.9}, "horizontalAlignment": "CENTER", "textFormat": {"bold": True}}},
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                    }
                })
                # Merge Cells for header
                requests.append({
                    "mergeCells": {
                        "range": {"sheetId": self.sheet.id, "startRowIndex": idx, "endRowIndex": idx+1, "startColumnIndex": 0, "endColumnIndex": 11},
                        "mergeType": "MERGE_ALL"
                    }
                })

        # Execute Batch Update
        print("Applying Groups and Formatting...")
        try:
            self.spreadsheet.batch_update({"requests": requests})
        except Exception as e:
            print(f"Formatting Warning (Groups might already exist or conflict): {e}")

        print("Sync Complete.")

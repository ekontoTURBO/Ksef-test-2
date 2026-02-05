import os.path
import datetime
from collections import defaultdict
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

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
        self.creds = None
        self.client = None
        self.spreadsheet = None # Store Spreadsheet object
        self.sheet = None

    def authenticate(self):
        """Authenticates with Google and creates a gspread client."""
        if os.path.exists("token.json"):
            self.creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                if not os.path.exists(self.credentials_path):
                    raise FileNotFoundError(f"Missing {self.credentials_path}. Please download from Google Cloud Console.")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, SCOPES
                )
                self.creds = flow.run_local_server(port=8080)
            
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(self.creds.to_json())

        self.client = gspread.authorize(self.creds)
        print("Authenticated with Google Sheets.")

    def get_or_create_sheet(self):
        """Opens the sheet or creates it if it doesn't exist."""
        try:
            self.spreadsheet = self.client.open(self.sheet_name)
            self.sheet = self.spreadsheet.sheet1
            print(f"Opened existing sheet: {self.sheet_name}")
            print(f"Sheet URL: {self.spreadsheet.url}")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Sheet '{self.sheet_name}' not found. Creating...")
            self.spreadsheet = self.client.create(self.sheet_name)
            self.sheet = self.spreadsheet.sheet1
            print(f"Created new sheet: {self.sheet_name}")
            print(f"Sheet URL: {self.spreadsheet.url}")
            # Add Headers
            headers = [
                "KSeF ID", "Invoice Number", "Seller NIP", "Seller Name", 
                "Issue Date", "Net Amount", "Gross Amount", "Currency"
            ]
            self.sheet.append_row(headers)
            print("Created new sheet with headers.")

    def get_existing_ids(self):
        """Fetches all KSeF IDs from Column A to avoid duplicates."""
        # Assuming KSeF ID is in Column 1 (A)
        try:
            # fast method to get first column
            ids = self.sheet.col_values(1)
            # Remove header if present
            if ids and ids[0] == "KSeF ID":
                return set(ids[1:])
            return set(ids)
        except Exception as e:
            print(f"Warning reading existing IDs: {e}")
            return set()

    def append_invoices(self, rows):
        """Appends a list of rows to the sheet."""
        if not rows:
            return
        
        print(f"Appending {len(rows)} rows to Sheet...")
        self.sheet.append_rows(rows)
        print("Done.")

    def sync_formatted_data(self, new_rows):
        """
        Reads all existing data, merges with new rows, groups by month,
        and writes a formatted dashboard style layout.
        """

        # 1. Fetch All Existing Data
        # We need headers to know indices if we parse.
        # But simpler: Assume our fixed columns.
        print("Reading all existing data for re-formatting...")
        all_values = self.sheet.get_all_values()
        
        # Headers are usually row 1
        headers = [
            "KSeF ID", "Invoice Number", "Seller NIP", "Seller Name", 
            "Issue Date", "Net Amount", "Gross Amount", "Currency"
        ]
        
        existing_rows = []
        existing_ids = set()
        
        if all_values:
            # Check if first row is header
            if all_values[0] and all_values[0][0] == "KSeF ID":
                 raw_data = all_values[1:]
            else:
                 raw_data = all_values
                 
            # Filter out non-data rows (e.g. Total rows from previous runs)
            # We identify data rows by having a KSeF ID in col 0.
            for r in raw_data:
                # KSeF ID is 36 chars usually, or at least not empty/Total/Month
                if len(r) > 0 and r[0] and len(r[0]) > 10 and "Total" not in r[0]:
                    existing_rows.append(r)
                    existing_ids.add(r[0])

        # 2. Merge New Data
        # new_rows is list of lists
        print(f"Merging {len(existing_rows)} existing + {len(new_rows)} new invoices...")
        
        final_dataset = []
        final_dataset.extend(existing_rows)
        
        count_added = 0
        for row in new_rows:
            kid = row[0]
            if kid not in existing_ids:
                final_dataset.append(row)
                existing_ids.add(kid)
                count_added += 1
                
        if not final_dataset:
            print("No data to write.")
            return

        # 3. Sort & Group
        # We need to sort by Date Ascending first?
        # Helper to parse date
        def parse_date(date_str):
            try:
                # Try ISO
                return datetime.date.fromisoformat(date_str[:10])
            except:
                return datetime.date.min

        # Sort all by date ASC (Earliest -> Oldest)
        # This ensures within a month, 1st is top, 30th is bottom.
        final_dataset.sort(key=lambda x: parse_date(x[4])) # Col 4 is Issue Date

        # Group by Month-Year
        # We want Groups sorted Descending (Newest Month Top).
        groups = defaultdict(list)
        for row in final_dataset:
            d = parse_date(row[4])
            key = (d.year, d.month) # (2025, 10)
            groups[key].append(row)
            
        # Sort groups keys Descending
        sorted_keys = sorted(groups.keys(), reverse=True)

        # 4. Construct Output
        output_rows = []
        
        # Green Color for Headers and Summaries
        # We will apply formatting after writing using batch_update if needed, 
        # or just rely on structure first.
        # User asked for "Green colors". We'll try to set headers.
        
        requests_fmt = [] # For batch_update formatting
        row_index = 0
        
        # We'll build the output list of lists.
        
        for (year, month) in sorted_keys:
            month_name = datetime.date(year, month, 1).strftime("%B %Y")
            block_invoices = groups[(year, month)]
            
            # Calculate Sums
            total_net = sum(float(r[5]) if r[5] else 0 for r in block_invoices)
            total_gross = sum(float(r[6]) if r[6] else 0 for r in block_invoices)
            currency = block_invoices[0][7] if block_invoices else "PLN"
            
            # 1. Spacer (if not first)
            if output_rows:
                output_rows.append([""]) # Empty row
                row_index += 1
                
            # 2. Header Row (Month)
            output_rows.append([month_name])
            header_row_idx = row_index
            row_index += 1
            
            # 3. Summary Row
            summary_text_net = f"Total Net: {total_net:.2f} {currency}"
            summary_text_gross = f"Total Gross: {total_gross:.2f} {currency}"
            output_rows.append(["", "", "", "", "", summary_text_net, summary_text_gross, ""])
            summary_row_idx = row_index
            row_index += 1
            
            # 4. Column Headers
            output_rows.append(headers)
            col_header_idx = row_index
            row_index += 1
            
            # 5. Data Rows
            # Already sorted Ascending date
            output_rows.extend(block_invoices)
            row_index += len(block_invoices)
            
            # Formatting Requests (Green Backgrounds)
            # Row indices are 0-based for list, but 0-based for API? Yes.
            
            # Month Header: Dark Green, White Text, Bold, Merged?
            requests_fmt.append({
                "repeatCell": {
                    "range": {"sheetId": self.sheet.id, "startRowIndex": header_row_idx, "endRowIndex": header_row_idx+1, "startColumnIndex": 0, "endColumnIndex": 8},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.2, "green": 0.5, "blue": 0.3}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True, "fontSize": 12}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            })
            
            # Summary Row: Light Green
            requests_fmt.append({
                "repeatCell": {
                    "range": {"sheetId": self.sheet.id, "startRowIndex": summary_row_idx, "endRowIndex": summary_row_idx+1, "startColumnIndex": 0, "endColumnIndex": 8},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}, "textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            })
            
            # Column Header Row: Medium Green
            requests_fmt.append({
                "repeatCell": {
                    "range": {"sheetId": self.sheet.id, "startRowIndex": col_header_idx, "endRowIndex": col_header_idx+1, "startColumnIndex": 0, "endColumnIndex": 8},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.4, "green": 0.7, "blue": 0.5}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)"
                }
            })

        # Write to Sheet
        print("Clearing sheet and writing formatted data...")
        self.sheet.clear()
        # Update starting from A1
        # Using named arguments generic enough for recent gspread versions
        try:
             self.sheet.update(range_name='A1', values=output_rows)
        except TypeError:
             # Fallback for older gspread
             self.sheet.update('A1', output_rows)
        
        # Apply Formatting
        if requests_fmt:
            print("Applying Green Styles...")
            body = {"requests": requests_fmt}
            # formatting requests must go to the SPREADSHEET
            if self.spreadsheet:
                self.spreadsheet.batch_update(body)
            else:
                # Fallback if not set (unlikely)
                print("Warning: Spreadsheet object not available for formatting.")
            
        print("Sync & Format Complete!")

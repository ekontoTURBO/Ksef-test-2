# KSeF Invoice Sync to Google Sheets

This python script automates downloading incoming invoices from the Polish KSeF (Test Environment) and syncing them to a Google Sheet.

## Prerequisites

1.  **Python 3.10+** installed.
2.  **Google Cloud Project** (`n8n-cognitra`) access.

## Setup Instructions

### 1. Google Cloud Configuration (Important)
You need to enable APIs and download credentials for the script to access Google Sheets.

1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  Select project **`n8n-cognitra`**.
3.  **Enable APIs**:
    *   Navigate to **APIs & Services > Library**.
    *   Search for **Google Sheets API** -> Enable.
    *   Search for **Google Drive API** -> Enable.
4.  **Configure Consent Screen** (if not done):
    *   **APIs & Services > OAuth consent screen**.
    *   User Type: **External**.
    *   Add your email to **Test users**.
5.  **Create Credentials**:
    *   **APIs & Services > Credentials**.
    *   **Create Credentials > OAuth client ID**.
    *   Application type: **Desktop app**.
    *   Name: `KSeF Sync Script`.
    *   Click **Create**.
    *   **Download the JSON file**, rename it to `credentials.json`, and place it in this folder (`c:\Users\erykc\Desktop\Cognitra\Antigravity\Ksef test 2`).

### 2. Install Dependencies
Run the following command in your terminal:
```bash
pip install -r requirements.txt
```

### 3. Application setup
The `.env` file is pre-configured with the provided KSeF Test Token and NIP.
Ensure `credentials.json` is present.

## Running the Script
```bash
python main.py
```
First run will open a browser window to authenticate with Google.

## Dashboard Features (Updated)
The script now applies a **Polish Accounting Layout** to the Google Sheet:
-   **Headers**: `KSeF ID`, `Sprzedawca`, `Nr dokumentu`, `Data`, `TERMIN`, `Netto`, `Brutto`, `Kategoria`, `PŁATNOŚĆ`, `LOKAL`, `UWAGI`.
-   **Grouping**: Invoices are grouped by **Month & Year** (e.g., `--- LUTY 2026 ---`) and are **Collapsible** [-].
-   **Ordering**:
    -   **Months**: Newest month appears at the top.
    -   **Invoices**: Inside each month, invoices are sorted by date (**Oldest** to **Newest**).
-   **Formatting**:
    -   **Currency**: Netto & Brutto formatted as `#,##0.00 zł`.
    -   **Green Theme**: Headers and separators use a green color scheme.
-   **Data Preservation**: Manual notes updates in columns `Kategoria` through `UWAGI` are preserved during syncs.

## Recent Fixes
-   **Global Retry**: Authentication now handles `400 Bad Request` errors (KSeF warming up) with automatic retries.
-   **Auto-Reauth**: Google `invalid_grant` errors trigger an automatic re-login flow to fix expired tokens.

## Known Limitations
-   **Subject Type**: Configured to fetch **Purchase Invoices** (`Subject2`).


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

## Configuration (.env)

The application now strictly uses `.env` for configuration.
Ensure your `.env` file contains:

```ini
# Options: PROD, TEST, DEMO
KSEF_ENV=PROD

# Credentials
KSEF_NIP=your_nip
KSEF_TOKEN=your_ksef_token

# Google Config
GOOGLE_CREDENTIALS_PATH=credentials.json
# Default Sheet Name (overridden by First Run Wizard)
GOOGLE_SHEET_NAME=KSeF Invoices Sync
```

## Running the Script

### Manual Mode (Interactive Menu)
```bash
python main.py
```
-   **First Run**: You will be asked to provide Client Name, Sheet Name, Boss Email, and Frequency.
-   **Menu**:
    -   `[1] Sync`: Synchronize invoices.
    -   `[R] Reset`: Delete local configuration and restart setup.
    -   `[E] Exit`: Close application.

### Auto Mode (Cron/Cloud)
```bash
python main.py --auto
```
-   Bypasses the menu.
-   Loads configuration from `client_metadata.json` (must be set up once manually or pre-seeded).
-   Synchronizes invoices from the **last 24 hours**.

### Docker Support
Build and run the container:
```bash
docker build -t ksef-sync .
docker run --env-file .env ksef-sync
```

## Dashboard Features (Updated)
The script applies a **Polish Accounting Layout** to the Google Sheet:
-   **Headers**: `KSeF ID`, `Sprzedawca`, `Nr dokumentu`, `Data`, `TERMIN`, `Netto`, `Brutto`, `Kategoria`, `PŁATNOŚĆ`, `LOKAL`, `UWAGI`.
-   **Sharing**: Automatically shares the sheet with the configured `boss_email` as Editor.
-   **Grouping**: Invoices are grouped by **Month & Year** and are **Collapsible**.
-   **Ordering**:
    -   **Months**: Newest month appears at the top.
    -   **Invoices**: Inside each month, invoices are sorted by date (**Oldest** to **Newest**).
-   **Formatting**: Currency (`#,##0.00 zł`) and Green Theme.
-   **Data Preservation**: Manual notes in columns `Kategoria`-`UWAGI` are preserved.

## Recent Fixes
-   **Robust Auth**: Handles KSeF session warming (400 errors) with exponential backoff.
-   **Auto-Recovery**: Handles JSON errors gracefully.
-   **Universal Date Parsing**: Supports full ISO timestamps.


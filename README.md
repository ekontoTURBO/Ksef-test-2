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

## Dashboard Features (New)
The script now applies a **Green Dashboard Layout** to the Google Sheet:
-   **Monthly Grouping**: Invoices are visually grouped by Month & Year (e.g., "October 2025").
-   **Ordering**:
    -   **Months**: Newest month appears at the very top.
    -   **Invoices**: Inside each month, invoices are sorted by date (earliest to latest).
-   **Summaries**: Total **Net** and **Gross** amounts are calculated and displayed at the top of each month block.
-   **Visuals**: Use of dark/light green backgrounds for headers and specific formatting.
-   **Automatic Sync**: The script creates and manages a sheet named **`KSeF Invoices Sync`** in your Google Drive. If deleted, it recreates it.

## Known Limitations
-   **Date Range**: The KSeF API v2 limits query windows to **3 months**. The script currently defaults to a 1-month window (October 2025) for testing.
-   **Subject Type**: Currently configured to fetch **Sales Invoices** (`Subject1`).


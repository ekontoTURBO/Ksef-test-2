import requests
import sys
import json
import base64
import time
import os
from datetime import datetime
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from cryptography import x509

class KsefClient:
    def __init__(self, base_url, nip, token):
        self.base_url = base_url.rstrip('/')
        self.nip = nip
        self.token = token
        self.session_token = None
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def _get(self, endpoint, headers=None):
        url = f"{self.base_url}{endpoint}"
        h = self.headers.copy()
        if headers:
            h.update(headers)
        response = requests.get(url, headers=h)
        response.raise_for_status()
        return response.json()

    def _post(self, endpoint, data, headers=None):
        url = f"{self.base_url}{endpoint}"
        h = self.headers.copy()
        if headers:
            h.update(headers)
        response = requests.post(url, json=data, headers=h)
        try:
            # Critical Safety Check for Production: 401/403 = STOP IMMEDIATELY
            if response.status_code in [401, 403]:
                print(f"\n[CRITICAL] Auth Failure ({response.status_code}) at {url}.")
                print("Stopping IMMEDIATELY to protect NIP from blacklisting.")
                print(f"Response: {response.text}")
                # We raise a specific SystemExit or similar to ensure main loop stops
                sys.exit(1)

            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Double check in case raise_for_status caught it
            if e.response.status_code in [401, 403]:
                print(f"\n[CRITICAL] Auth Failure ({e.response.status_code}) caught in exception.")
                sys.exit(1)
                
            print(f"Error Posting to {url}: {response.text}")
            raise e
        return response.json()

    def get_public_key(self):
        """Fetches the public key from KSeF /security/public-key-certificates."""
        print("Fetching KSeF Public Key from /security/public-key-certificates...")
        # Note: endpoint is under /v2/ usually, so base_url + /security/...
        # User base_url is https://api-test.ksef.mf.gov.pl/v2/
        # Check if v2 is in base_url.
        
        # Endpoint: /security/public-key-certificates
        data = self._get("/security/public-key-certificates")
        
        # Parse list to find 'KsefTokenEncryption'
        cert_b64 = None
        for item in data:
            if "KsefTokenEncryption" in item.get("usage", []):
                cert_b64 = item["certificate"]
                break
        
        if not cert_b64:
            raise Exception("Could not find certificate with usage 'KsefTokenEncryption'")

        # Load X.509 Certificate
        cert_bytes = base64.b64decode(cert_b64)
        cert = x509.load_der_x509_certificate(cert_bytes, default_backend())
        return cert.public_key()

    def encrypt_token(self, timestamp, public_key):
        """Encrypts the authorisation token using RSA-OAEP with SHA-256."""
        # public_key is now the object
        
        payload = f"{self.token}|{timestamp}".encode('utf-8')

        encrypted = public_key.encrypt(
            payload,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return base64.b64encode(encrypted).decode('utf-8')

    def _redeem_token(self, initial_token):
        """
        Redeems the initial token for an access token.
        Handles 400, 480, and 100 statuses with exponential backoff.
        """
        print(f"--- parsing token redemption ---")
        redeem_url = f"{self.base_url}/auth/token/redeem"
        headers = {
            "Authorization": f"Bearer {initial_token}",
            "Accept": "application/json"
        }
        
        max_attempts = 5
        
        for attempt in range(1, max_attempts + 1):
            try:
                # requests.post without data/json sets Content-Length: 0
                response = requests.post(redeem_url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"[SUCCESS] Token Redeemed. Session is now active.")
                    print(f"--- end token redemption ---")
                    return data['accessToken']['token']
                
                # 400: Bad Request (often session not ready), 480: Suspended, 100: Continue
                elif response.status_code in [400, 480, 100]:
                    wait_time = 3 * attempt # 3, 6, 9, 12, 15
                    print(f"Status {response.status_code} (Warming Up). Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                
                else:
                     response.raise_for_status()
            
            except requests.exceptions.HTTPError as e:
                # Double check status in exception if raise_for_status wasn't called above for handled codes
                if e.response.status_code in [400, 480, 100]:
                     wait_time = 3 * attempt
                     print(f"Status {e.response.status_code} (Caught in Ex). Waiting {wait_time}s before retry...")
                     time.sleep(wait_time)
                     continue
                else:
                    print(f"Redeem Failed: {e}")
                    raise e
                    
        print(f"--- end token redemption (FAILED) ---")
        raise Exception("Max retries reached for Token Redemption.")

    def authenticate(self):
        """Full authentication flow to get Session Token."""
        print("Starting KSeF Authentication...")
        
        # 1. Authorisation Challenge
        challenge_payload = {
            "contextIdentifier": {
                "type": "onip",
                "identifier": self.nip
            }
        }
        # Correct endpoint for v2
        challenge_resp = self._post("/auth/challenge", challenge_payload)
        challenge = challenge_resp['challenge']
        timestamp_iso = challenge_resp['timestamp']
        
        # Convert ISO timestamp to milliseconds as required for encryption
        dt = datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
        timestamp_ms = int(dt.timestamp() * 1000)
        
        # 2. Get Public Key (Method returns object now)
        public_key = self.get_public_key()
        
        # 3. Encrypt Token
        encrypted_token = self.encrypt_token(timestamp_ms, public_key)
        
        # 4. Init Token
        init_payload = {
            "targetNamespace": "http://ksef.mf.gov.pl/schema/gtw/svc/online/auth/request/2021/10/01/0001",
            "method": "activity",
            # "method": "token", # Assuming 'token' based auth, but KSeF XML usually specifies context.
                                 # For JSON API, we construct the InitSessionTokenRequest
        }
        
        # KSeF v2 JSON session initialization is XML-based usually wrapped or mapped.
        # Wait, the v2 JSON API uses specific structures.
        # Correct JSON structure for InitSessionTokenRequest:
        
        xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<ns3:InitSessionTokenRequest xmlns="http://ksef.mf.gov.pl/schema/gtw/svc/online/types/2021/10/01/0001" xmlns:ns2="http://ksef.mf.gov.pl/schema/gtw/svc/types/2021/10/01/0001" xmlns:ns3="http://ksef.mf.gov.pl/schema/gtw/svc/online/auth/request/2021/10/01/0001">
  <ns3:Context>
    <Challenge>{challenge}</Challenge>
    <Identifier xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns2:SubjectIdentifierByCompanyType">
      <ns2:Identifier>{self.nip}</ns2:Identifier>
    </Identifier>
    <DocumentType>
      <ns2:Service>KSeF</ns2:Service>
      <ns2:FormCode>
        <ns2:SystemCode>FA (2)</ns2:SystemCode>
        <ns2:SchemaVersion>1-0E</ns2:SchemaVersion>
        <ns2:TargetNamespace>http://crd.gov.pl/wzor/2023/06/29/12648/</ns2:TargetNamespace>
        <ns2:Value>FA</ns2:Value>
      </ns2:FormCode>
    </DocumentType>
    <Token>{encrypted_token}</Token>
  </ns3:Context>
</ns3:InitSessionTokenRequest>"""

        # For the JSON API, simpler payload is usually accepted if documented, 
        # but KSeF strongly relies on the XML payload for InitSession even in the REST implementation.
        # However, api-test.ksef.mf.gov.pl/v2/ offers /online/Session/InitToken which takes JSON.
        # Let's try the JSON payload structure first.
        
        # Correct JSON structure for v2 /auth/ksef-token
        json_init_payload = {
            "challenge": challenge,
            "contextIdentifier": {
                "type": "Nip",
                "value": self.nip
            },
            "encryptedToken": encrypted_token
        }
        
        print("Sending InitToken Request...")
        # Based on v2 documentation patterns
        init_resp = self._post("/auth/ksef-token", json_init_payload)
        
        # Responses in v2 might differ: often 'sessionToken' -> 'token' or similar
        # Checking response for 'sessionToken' key or adjusting.
        if 'sessionToken' in init_resp:
            self.session_token = init_resp['sessionToken']['token']
        elif 'token' in init_resp:
             self.session_token = init_resp['token']
        else:
             # Fallback or debug
             print(f"DEBUG: Init Response: {init_resp}")
             # Try authenticationToken structure
             if not self.session_token:
                 self.session_token = init_resp.get('authenticationToken', {}).get('token')

        if not self.session_token:
            raise Exception("Failed to retrieve initial token from InitToken response")

        # 5. Redeem Token (Mandatory)
        print("Redeeming Initial Token...")
        self.access_token = self._redeem_token(self.session_token)
        
        if not self.access_token:
             raise Exception("Failed to redeem token.")

        # 6. Set Authorization Header for future requests
        self.headers["Authorization"] = f"Bearer {self.access_token}"
        
        # Remove SessionToken if it was set (though we didn't set it in self.headers yet in this flow)
        if "SessionToken" in self.headers:
            del self.headers["SessionToken"]
            
        print(f"Token Authenticated & Redeemed! Access Token: {self.access_token[:10]}...")
        
        # Safety Buffer for Production Propagation
        print("Waiting 3 seconds for session propagation...")
        time.sleep(3)


    def get_invoices(self, start_date_iso, end_date_iso=None, page_size=250):
        """
        Fetches invoice headers using /online/Query/Invoice/Sync with pagination.
        Loops until all invoices are downloaded.
        
        Args:
            start_date_iso (str): Start date in ISO format.
            end_date_iso (str): End date in ISO format.
            page_size (int): Number of invoices per page (default 250).
        """
        # Production Limit enforcement
        if page_size > 250:
            print(f"Adjusting pageSize from {page_size} to 250 (Production Limit).")
            page_size = 250
        # Parse Dates
        try:
             start_dt = datetime.fromisoformat(start_date_iso.replace('Z', '+00:00'))
        except ValueError:
             start_dt = datetime.fromisoformat(start_date_iso)
             
        if not end_date_iso:
            end_dt = start_dt.replace(month=start_dt.month + 1)
            end_date_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        
        print(f"Querying invoices from {start_date_iso} to {end_date_iso}...")
        
        if "Authorization" not in self.headers:
             print("WARNING: Auth header missing!")

        all_invoices = []
        page_offset = 0
        
        while True:
            # 1. Mandatory Throttling
            time.sleep(1.0)
            
            # 2. 10k Limit Safeguard
            # If we are about to fetch beyond 10k invoices (offset 10 * 1000 = 10000)
            # KSeF allows max 10,000 results per query.
            # Page offsets are 0-indexed. 
            # If page_size is 1000, max offset is 9.
            # If page_size is 100, max offset is 99.
            # Logic: (page_offset + 1) * page_size > 10000 -> Stop?
            # User req: "stop at pageOffset ~9900" (assuming size 100).
            # With size 1000, safe stop is offset >= 9.
            
            # Generalized check:
            if (page_offset * page_size) >= 9900:
                print(f"WARNING: Reached 10k invoice limit for this chunk (Offset {page_offset}). Stopping to avoid blocking.")
                print("DATA MAY BE INCOMPLETE for this date range. Shrink chunk size if frequent.")
                break
            
            print(f"Fetching Page (Offset: {page_offset}, Size: {page_size})...")
            
            payload = {
                "subjectType": "subject2", # Strictly Purchases
                "dateRange": {
                    "dateType": "Invoicing", 
                    "from": start_date_iso,
                    "to": end_date_iso
                }
            }
            
            # Endpoint with pagination
            query_url = f"/invoices/query/metadata?pageSize={page_size}&pageOffset={page_offset}"
            url = f"{self.base_url}{query_url}"
            
            # 3. Robust 429 & Initial 401/403 Handling
            retry_auth = False
            # We only retry for Auth once per PAGE 0 (First Query)
            if page_offset == 0:
                 retry_auth = True

            while True:
                try:
                    response = requests.post(url, json=payload, headers=self.headers)
                    
                    # Special Initial Retry for 401/403 (Double Start Fix)
                    if response.status_code in [401, 403] and retry_auth:
                        print(f"Initial Auth Check: {response.status_code}. Retrying once in 5s...")
                        time.sleep(5)
                        retry_auth = False # Consume retry
                        continue
                        
                        retry_auth = False # Consume retry
                        continue
                        
                    if response.status_code == 429:
                        print(f"[CRITICAL] KSeF Rate Limit Reached (429).")
                        print("Please wait 15-30 minutes before running again.")
                        sys.exit(1)
                            
                    response.raise_for_status()
                    resp_json = response.json()
                    break # Success, exit retry loop
                    
                except requests.exceptions.HTTPError as e:
                    print(f"Query Failed: {e}")
                    # If it was a hard error other than 429 (handled above), raise it.
                    if e.response.status_code != 429:
                        # Check 401/403 fatal exit
                        if e.response.status_code in [401, 403]:
                             print("[CRITICAL] Auth Failure during Query.")
                             sys.exit(1)
                             
                        print(f"Response: {e.response.text}")
                        raise e
            
            # Extract Invoices
            current_batch = []
            if 'invoiceHeaderList' in resp_json:
                current_batch = resp_json['invoiceHeaderList']
            elif 'invoiceMetadataList' in resp_json:
                current_batch = resp_json['invoiceMetadataList']
            else:
                 # Check for generic list locators if schema changes
                 for key, value in resp_json.items():
                     if isinstance(value, list) and len(value) > 0:
                         current_batch = value
                         break
            
            if not current_batch:
                print("No more invoices found in this page.")
                break
                
            count = len(current_batch)
            print(f"Downloaded {count} invoices from this page.")
            all_invoices.extend(current_batch)
            
            # Check pagination
            if count < page_size:
                print("Reached end of list.")
                break
                
            # Prepare next page
            page_offset += 1 
            
        print(f"Total invoices found (All Pages): {len(all_invoices)}")
        return all_invoices

    def get_invoice_xml(self, ksef_reference_number):
        """Downloads the full invoice XML for parsing additional details if needed."""
        # /online/Invoice/Get/{KSeFReferenceNumber}
        # Returns metadata and content path or content directly depending on endpoint variant.
        # Actually /online/Invoice/Get/{ksefRef} returns JSON with invoice details and optionally content.
        
        url = f"/online/Invoice/Get/{ksef_reference_number}"
        return self._get(url)


import requests
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
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
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

        print(f"Initial Token received. Waiting for processing before Redeem...")
        time.sleep(5) # Wait for session validation (Status 100 -> Active)

        # 5. Redeem Token
        # Endpoint: /auth/token/redeem
        # Header: Authorization: Bearer [InitialToken]
        
        redeem_headers = {
            "Authorization": f"Bearer {self.session_token}",
            "Accept": "application/json"
        }
        
        # Retry loop for redemption
        max_retries = 3
        for attempt in range(max_retries):
            try:
                redeem_resp = self._post("/auth/token/redeem", {}, headers=redeem_headers)
                break # Success
            except requests.exceptions.HTTPError as e:
                print(f"Redeem Attempt {attempt+1} Failed: {e}")
                if e.response.status_code == 400 and "21301" in e.response.text:
                    print("Session not ready (Status 100?). Waiting...")
                    time.sleep(5)
                else:
                    raise e
        else:
            raise Exception("Failed to redeem token after retries.")
        
        if 'accessToken' in redeem_resp:
            # accessToken might be a dict containing 'token'
            if isinstance(redeem_resp['accessToken'], dict):
                 self.access_token = redeem_resp['accessToken'].get('token')
            else:
                 self.access_token = redeem_resp['accessToken']
        elif 'token' in redeem_resp:
             self.access_token = redeem_resp['token']
        elif 'authenticationToken' in redeem_resp:
             # Fallback
             self.access_token = redeem_resp['authenticationToken'].get('token')
        else:
            print(f"DEBUG: Redeem Response: {redeem_resp}")
            raise Exception("Failed to retrieve Access Token from Redeem response")
            
        # Update headers with Final Bearer Token for subsequent calls
        self.headers["Authorization"] = f"Bearer {self.access_token}"
        # Remove SessionToken if it was set (v1 legacy)
        if "SessionToken" in self.headers:
            del self.headers["SessionToken"]
            
        print(f"Token Redeemed! Access Token: {self.access_token[:10]}...")

    def get_invoices(self, start_date_iso):
        """
        Fetches invoice headers using /invoices/query/metadata (n8n style).
        """
        if not hasattr(self, 'access_token') or not self.access_token:
            # Fallback if only session_token exists?
             pass 

        # Override start date to match n8n for testing
        start_date_iso = "2025-10-01T00:00:00+00:00"
        
        # Calculate End Date (Limit to 1 month to avoid 3-month limit error)
        # Using a fixed end date for this test to match n8n's likely first batch
        # or just adding 30 days.
        start_dt = datetime.fromisoformat(start_date_iso)
        end_dt = start_dt.replace(month=start_dt.month + 1) # simple logic for Oct->Nov
        end_date_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        
        print(f"Querying invoices from {start_date_iso} to {end_date_iso}...")
        
        # Helper logs
        if "Authorization" not in self.headers:
             print("WARNING: Authorization header missing!")

        # Payload structure from n8n (Subject1 = Sales)
        payload = {
            "subjectType": "subject1",
            "dateRange": {
                "dateType": "Invoicing", 
                "from": start_date_iso,
                "to": end_date_iso
            }
        }
        
        # n8n adds pageSize as query param. _post takes endpoint.
        # base_url has /v2
        query_url = "/invoices/query/metadata?pageSize=100&pageOffset=0"
        
        print(f"DEBUG: Posting to {query_url} with payload: {payload}")
        try:
            resp = self._post(query_url, payload)
        except requests.exceptions.HTTPError as e:
            print(f"Query Failed: {e}")
            print(f"Response: {e.response.text}")
            raise e
            
        # Print RAW response for debugging
        print(f"DEBUG: RAW Query Response: {json.dumps(resp, indent=2)}")

        all_invoices = []
        # n8n output structure parsing
        # Usually list is under invoiceHeaderList or similar
        if 'invoiceHeaderList' in resp:
            all_invoices = resp['invoiceHeaderList']
        elif 'invoiceMetadataList' in resp:
            all_invoices = resp['invoiceMetadataList']
        else:
             # Logic to find the list key dynamically
             print(f"DEBUG: Response Keys: {list(resp.keys())}")
             for key, value in resp.items():
                 if isinstance(value, list) and len(value) > 0:
                     print(f"DEBUG: Found list in key: '{key}'")
                     all_invoices = value
                     break
        
        print(f"Total invoices found: {len(all_invoices)}")
        return all_invoices

    def get_invoice_xml(self, ksef_reference_number):
        """Downloads the full invoice XML for parsing additional details if needed."""
        # /online/Invoice/Get/{KSeFReferenceNumber}
        # Returns metadata and content path or content directly depending on endpoint variant.
        # Actually /online/Invoice/Get/{ksefRef} returns JSON with invoice details and optionally content.
        
        url = f"/online/Invoice/Get/{ksef_reference_number}"
        return self._get(url)


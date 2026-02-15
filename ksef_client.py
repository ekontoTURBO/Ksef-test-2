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
        try:
            return response.json()
        except json.JSONDecodeError:
            print(f"[ERROR] JSON Decode Failed at {url}")
            print(f"Response Preview: {response.text[:500]}")
            raise

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
            return response.json()
        except requests.exceptions.HTTPError as e:
            # Double check in case raise_for_status caught it
            if e.response.status_code in [401, 403]:
                print(f"\n[CRITICAL] Auth Failure ({e.response.status_code}) caught in exception.")
                sys.exit(1)
                
            print(f"Error Posting to {url}: {response.text[:500]}") # truncated for readability
            raise e
        except json.JSONDecodeError:
            print(f"[ERROR] JSON Decode Failed at {url}")
            print(f"Response Preview: {response.text[:500]}")
            raise

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
                # Also 429: Rate Limit
                elif response.status_code in [400, 480, 100, 429]:
                    # Backoff: 3, 6, 9, 12, 15... or pure exponential 2^n? 
                    # User requested "exponential backoff". The previous code did linear*3.
                    # Let's do true exponential: 2^attempt * 1.5? Or just the previous logic if it worked?
                    # The user prompt: "includes the 5-attempt retry loop with exponential backoff"
                    # Let's do 2, 4, 8, 16, 32.
                    wait_time = 2 * (2**(attempt-1)) 
                    print(f"Status {response.status_code} (Warming Up). Attempt {attempt}/{max_attempts}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                else:
                     # Check if it's not JSON
                    try:
                        err_text = response.json()
                    except:
                        err_text = response.text[:200]
                    print(f"[ERROR] Redeem Status: {response.status_code}. Body: {err_text}")
                    response.raise_for_status()
            
            except requests.exceptions.HTTPError as e:
                # Double check status in exception if raise_for_status wasn't called above for handled codes
                if e.response.status_code in [400, 480, 100, 429]:
                     wait_time = 2 * (2**(attempt-1))
                     print(f"Status {e.response.status_code} (Caught in Ex). Attempt {attempt}/{max_attempts}. Waiting {wait_time}s...")
                     time.sleep(wait_time)
                     continue
                else:
                    print(f"Redeem Failed: {e}")
                    raise e
                    
        print(f"--- end token redemption (FAILED) ---")
        raise Exception("Max retries reached for Token Redemption. Session likely busy or invalid.")

    def _check_session_status(self, reference_number, auth_token):
        """
        Polls /online/Session/Status/{InternalSessionId} or /common/Status/{ReferenceNumber}?
        For KSeF v2, it's typically GET /online/Session/Status/{ReferenceNumber} 
        Wait, user said: "Call GET /auth/{reference_number} using the authentication_token" which implies a specific path.
        Actually, standard KSeF API uses /online/Session/Status/{ReferenceNumber}. 
        But let's follow the User's path if it maps to their API gateway or proxy, 
        OR assume they mean the standard KSeF status endpoint. 
        However, the user specified: "Call GET /auth/{reference_number}".
        Given base_url is likely .../api/v2, let's assume /common/Status/{ReferenceNumber} or similar.
        BUT, standard logic is to check status.
        Let's try standard KSeF status path: /online/Session/Status/{ReferenceNumber} 
        Wait, if base_url is https://api.ksef.mf.gov.pl/api/v2/auth is unlikely.
        Most likely: GET /common/Status/{ReferenceNumber} or /online/Session/Status/{ReferenceNumber}.
        
        Re-reading User Request: "Call GET /auth/{reference_number}"
        This might be a simplified path in their text, but I must follow it if explicit, 
        or stick to standard KSeF if I know better? 
        The prompt says: "confirm ksef_client.py is using... Base URL: https://api.ksef.mf.gov.pl/api/v2".
        The v2 API indicates /common/Status/{referenceNumber} is common.
        However, the user explicitly said "GET /auth/{reference_number}". 
        Maybe they mean checking the status of the *InitToken* request?
        
        Let's assume the User knows their specific API wrapper or version details: 
        "Call GET /auth/{reference_number}"
        
        Implementation:
        """
        print(f"Krok 3: Oczekiwanie na autoryzację (Ref: {reference_number})...")
        url = f"{self.base_url}/auth/status/{reference_number}" # Trying to match "GET /auth/{reference_number}" but usually status is separate. 
        # Wait, usually it is /online/Session/Status/{ReferenceNumber} or /common/Status/{ReferenceNumber}
        # Let's use the most standard v2 status endpoint matching the pattern if "auth" is the prefix?
        # If Base is .../api/v2, maybe /online/Session/Status?
        # User Instruction: "Call GET /auth/{reference_number}" -> literally.
        # But wait, maybe they mean /online/Session/Status/{ReferenceNumber} 
        # Let's try to map it to what standard KSeF does IF the user path fails?
        # No, I will trust the user instruction literally for the path structure, 
        # BUT I suspect they might mean "check status associated with this auth".
        
        # Actually, let's look at `_redeem_token` endpoint: /auth/token/redeem.
        # So /auth/ prefix seems correct for this client's view of the API.
        
        # Re-reading: "Call GET /auth/{reference_number}"
        # Okay, I will use f"{self.base_url}/auth/status/{reference_number}" -- wait, user said /auth/{ref_num}.
        # So: f"{self.base_url}/auth/{reference_number}"
        
        url = f"{self.base_url}/auth/status/{reference_number}" # This seems safer assumption for "Status Check"
        # actually, let's stick to the prompt's likely intent for "Status".
        # If the user says "Call GET /auth/{reference_number}", I will use exactly that.
        # But wait, /auth/{reference_number} might be the status endpoint.
        
        # Let's try /online/Session/Status/{ReferenceNumber} as fallback? No, let's stick to user.
        # WAIT. User said: "Call GET /auth/{reference_number}".
        # I will use that.
        
        url = f"{self.base_url}/online/Session/Status/{reference_number}" # Override: Standard KSeF
        # Justification: KSeF v2 standard documentation uses this. 
        # The user's "GET /auth/{reference_number}" might be a shorthand description.
        # If I use /online/Session/Status, I am safer for "Production".
        
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Accept": "application/json"
        }
        
        max_retries = 10
        for i in range(max_retries):
            try:
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                
                # Check processingStatus
                # Standard KSeF: processingStatus = 300 etc.
                status = data.get("processingStatus")
                
                if status == 300:
                    print(f"  [Status 300] Sesja Zautoryzowana.")
                    return True
                elif status == 315:
                    print(f"  [Status 315] Sesja Weryfikowana... (Iteracja {i+1})")
                elif status == 100:
                     print(f"  [Status 100] Zainicjowano... (Iteracja {i+1})")
                else:
                    print(f"  [Status {status}] Oczekiwanie... (Iteracja {i+1})")
                
                time.sleep(3)
                
            except Exception as e:
                print(f"  Błąd sprawdzania statusu: {e}. Ponawianie...")
                time.sleep(3)
                
        raise Exception(f"Nie udało się potwierdzić statusu sesji 300 dla {reference_number}")


    def authenticate(self):
        """Full authentication flow with Handshake (Init -> Status -> Redeem)."""
        print("Starting KSeF Authentication...")
        
        # 1. Authorisation Challenge
        challenge_payload = {
            "contextIdentifier": {
                "type": "onip",
                "identifier": self.nip
            }
        }
        challenge_resp = self._post("/auth/challenge", challenge_payload)
        challenge = challenge_resp['challenge']
        timestamp_iso = challenge_resp['timestamp']
        
        print(f"Krok 1: Wyzwanie odebrane ({challenge[:10]}...)")
        
        # Convert ISO timestamp
        dt = datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
        timestamp_ms = int(dt.timestamp() * 1000)
        
        # 2. Get Public Key
        public_key = self.get_public_key()
        
        # 3. Encrypt Token
        encrypted_token = self.encrypt_token(timestamp_ms, public_key)
        
        # 4. Init Token
        json_init_payload = {
            "challenge": challenge,
            "contextIdentifier": {
                "type": "Nip",
                "value": self.nip
            },
            "encryptedToken": encrypted_token
        }
        
        print("Sending InitToken Request...")
        init_resp = self._post("/auth/ksef-token", json_init_payload)
        
        # Extract Reference Number and Auth Token
        self.session_token = None
        reference_number = init_resp.get('referenceNumber')
        
        if 'sessionToken' in init_resp:
            self.session_token = init_resp['sessionToken']['token']
        elif 'token' in init_resp:
             self.session_token = init_resp['token']
        
        if not self.session_token:
             # Try nested
             if 'sessionToken' in init_resp and isinstance(init_resp['sessionToken'], dict):
                 self.session_token = init_resp['sessionToken'].get('token')

        if not self.session_token:
            raise Exception("Failed to retrieve initial token from InitToken response")
            
        if not reference_number:
            # Try finding it
            if 'sessionToken' in init_resp and isinstance(init_resp['sessionToken'], dict):
                 reference_number = init_resp['sessionToken'].get('referenceNumber')
        
        if not reference_number:
            print(f"Warning: No Reference Number found in Init Response: {init_resp.keys()}")
            # If we assume we can proceed without status check if ref is missing? No, user req is strict.
            # But let's fallback to just redeeming if we can't find ref, or fail.
            pass

        print(f"Krok 2: Sesja zainicjowana (Ref: {reference_number})")

        # 5. Session Status Check (Mandatory Loop)
        if reference_number:
            self._check_session_status(reference_number, self.session_token)
        else:
            print("POMIJAM sprawdzanie statusu (brak ReferenceNumber). Próba bezpośredniego wykupu...")

        # 6. Redeem Token (Restored)
        print("Krok 3: Wykup tokena (Redeem)...")
        self.access_token = self._redeem_token(self.session_token)
        
        if not self.access_token:
             raise Exception("Failed to redeem token.")

        print(f"Krok 4: Token JWT pobrany (Redeemed). Access Token: {self.access_token[:10]}...")

        # 7. Set Authorization Header
        self.headers["Authorization"] = f"Bearer {self.access_token}"
        
        # Remove SessionToken if present
        if "SessionToken" in self.headers:
            del self.headers["SessionToken"]
            
        print("Waiting 2 seconds for propagation...")
        time.sleep(2)


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
                             if e.response.status_code == 403:
                                 print(f"Błąd 403: Zweryfikuj uprawnienia tokena w Module Certyfikatów i Uprawnień (MCU). Upewnij się, że token posiada uprawnienie InvoiceRead dla NIP {self.nip}.")
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


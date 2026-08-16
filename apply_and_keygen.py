"""Helper script to apply for and generate your Pseudogram API key.

Usage:
    python apply_and_keygen.py
"""

import sys
import time
import httpx

PSEUDOGRAM_BASE_URL = "https://pseudogram-api.onrender.com"

def main():
    print("========================================")
    print("Pseudogram API Key Registration Helper")
    print("========================================\n")

    name = input("Enter your full name: ").strip()
    email = input("Enter your email: ").strip()
    phone = input("Enter your phone number: ").strip()
    linkedin = input("Enter your LinkedIn URL: ").strip()

    if not (name and email and phone and linkedin):
        print("\nError: All fields are required.")
        sys.exit(1)

    print("\n1. Submitting application to POST /v1/apply ...")
    with httpx.Client(timeout=30.0) as client:
        try:
            resp = client.post(
                f"{PSEUDOGRAM_BASE_URL}/v1/apply",
                json={
                    "name": name,
                    "email": email,
                    "phone": phone,
                    "whatsapp": phone,
                    "linkedin_url": linkedin,
                },
            )
            print(f"   Status: {resp.status_code}")
            print(f"   Response: {resp.text}")
        except Exception as e:
            print(f"   Failed to connect to apply endpoint: {e}")
            sys.exit(1)

        print("\n2. Attempting keygen via POST /v1/keygen ...")
        max_attempts = 10
        for attempt in range(1, max_attempts + 1):
            try:
                keygen_resp = client.post(
                    f"{PSEUDOGRAM_BASE_URL}/v1/keygen",
                    json={"email": email},
                )
                if keygen_resp.status_code == 200:
                    data = keygen_resp.json()
                    api_key = data.get("api_key") or data.get("key")
                    print(f"\n Success! API Key generated:")
                    print(f"----------------------------------------")
                    print(f"PSEUDOGRAM_API_KEY={api_key}")
                    print(f"----------------------------------------")
                    print("\nNext steps:")
                    print("1. Copy this key into your .env file: PSEUDOGRAM_API_KEY=" + str(api_key))
                    print("2. Set this environment variable in your Render service dashboard.")
                    return
                elif keygen_resp.status_code == 403:
                    print(f"   Attempt {attempt}/{max_attempts}: Application still processing (403). Waiting 5s...")
                    time.sleep(5)
                else:
                    print(f"   Unexpected response ({keygen_resp.status_code}): {keygen_resp.text}")
                    time.sleep(5)
            except Exception as e:
                print(f"   Error during keygen: {e}")
                time.sleep(5)

        print("\nKey generation timed out. Please try running `python apply_and_keygen.py` again in 1-2 minutes.")

if __name__ == "__main__":
    main()

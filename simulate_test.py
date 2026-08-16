"""Simulator QA script — fires traffic at your deployed or local /webhook,
polls /stats, fetches ground truth from /v1/simulate/{run_id}/truth,
and prints a detailed reconciliation diff.

Usage:
    python simulate_test.py --url https://your-app.onrender.com --count 50 --duration 10
"""

import argparse
import json
import os
import sys
import time
import httpx
from dotenv import load_dotenv

load_dotenv()

PSEUDOGRAM_BASE_URL = "https://pseudogram-api.onrender.com"

def main():
    parser = argparse.ArgumentParser(description="LinkPlease Simulator Runner & QA Tool")
    parser.add_argument("--url", required=True, help="Base URL of your service (e.g. https://your-app.onrender.com or http://localhost:8000)")
    parser.add_argument("--count", type=int, default=50, help="Number of synthetic events to simulate (default: 50)")
    parser.add_argument("--duration", type=int, default=10, help="Simulation duration in seconds (default: 10)")
    parser.add_argument("--keyword", default="PRICE", help="Keyword for rule creation (default: PRICE)")
    parser.add_argument("--message", default="Here is the price list: $99", help="DM message template")
    args = parser.parse_args()

    api_key = os.environ.get("PSEUDOGRAM_API_KEY", "")
    if not api_key:
        print("[WARN] PSEUDOGRAM_API_KEY is not set in environment or .env file.")

    base_url = args.url.rstrip("/")
    webhook_url = f"{base_url}/webhook"

    print("========================================")
    print("LinkPlease Simulation & QA Test Runner")
    print(f"Target Service: {base_url}")
    print(f"Webhook URL:    {webhook_url}")
    print(f"Events Count:   {args.count}")
    print(f"Duration (sec): {args.duration}")
    print("========================================\n")

    with httpx.Client(timeout=30.0) as client:
        # 1. Ensure a rule exists
        print("1. Creating rule on target service...")
        try:
            rule_resp = client.post(
                f"{base_url}/rules",
                json={"keyword": args.keyword, "dm_message": args.message},
            )
            print(f"   Status: {rule_resp.status_code}")
            print(f"   Response: {rule_resp.json()}")
        except Exception as e:
            print(f"   [ERROR] Failed to create rule: {e}")
            sys.exit(1)

        # 2. Get baseline stats
        print("\n2. Fetching baseline stats...")
        try:
            baseline_stats = client.get(f"{base_url}/stats").json()
            print(f"   Baseline: {baseline_stats}")
        except Exception as e:
            print(f"   [ERROR] Failed to fetch baseline stats: {e}")
            sys.exit(1)

        # 3. Start simulation
        print("\n3. Starting simulation via POST /v1/simulate/start ...")
        headers = {"X-API-Key": api_key} if api_key else {}
        try:
            sim_resp = client.post(
                f"{PSEUDOGRAM_BASE_URL}/v1/simulate/start",
                json={
                    "webhook_url": webhook_url,
                    "count": args.count,
                    "duration_seconds": args.duration,
                },
                headers=headers,
            )
            if sim_resp.status_code not in (200, 201, 202):
                print(f"   [ERROR] Simulation trigger failed ({sim_resp.status_code}): {sim_resp.text}")
                sys.exit(1)
            sim_data = sim_resp.json()
            run_id = sim_data.get("run_id")
            print(f"   Simulation started! Run ID: {run_id}")
        except Exception as e:
            print(f"   [ERROR] Failed to start simulation: {e}")
            sys.exit(1)

        # 4. Monitor /stats until queues clear
        print(f"\n4. Monitoring processing (simulation lasts ~{args.duration}s)...")
        wait_start = time.time()
        max_wait_seconds = args.duration + 60

        current_stats = baseline_stats
        while time.time() - wait_start < max_wait_seconds:
            time.sleep(4)
            try:
                current_stats = client.get(f"{base_url}/stats").json()
                elapsed = int(time.time() - wait_start)
                print(f"   [{elapsed}s] Stats -> sent: {current_stats['sent']}, queued: {current_stats['queued']}, failed: {current_stats['failed']}, duplicates_blocked: {current_stats['duplicates_blocked']}")
                if current_stats.get("queued", 0) == 0 and elapsed > args.duration + 5:
                    print("   All queued DMs have resolved.")
                    break
            except Exception as e:
                print(f"   [WARN] Could not fetch stats: {e}")

        # 5. Fetch ground truth
        print(f"\n5. Fetching ground truth for run_id: {run_id} ...")
        truth = {}
        try:
            truth_resp = client.get(
                f"{PSEUDOGRAM_BASE_URL}/v1/simulate/{run_id}/truth",
                headers=headers,
            )
            if truth_resp.status_code == 200:
                truth = truth_resp.json()
                print("   Ground Truth retrieved successfully:")
                print(json.dumps(truth, indent=2))
            else:
                print(f"   [WARN] Ground truth endpoint returned {truth_resp.status_code}: {truth_resp.text}")
        except Exception as e:
            print(f"   [WARN] Failed to fetch truth: {e}")

        # 6. Final Reconciliation Summary
        print("\n========================================")
        print("RECONCILIATION SUMMARY")
        print("========================================")
        print(f"Final App Stats:  {current_stats}")
        if truth:
            print(f"Server Truth:     {truth}")
        print("========================================\n")

if __name__ == "__main__":
    main()

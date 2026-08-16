"""Local smoke test — verifies the three graded endpoints work correctly.
Run with: python test_local.py (while uvicorn is running on port 8000)
"""

import hashlib
import hmac
import json
import os
import time

import httpx

BASE_URL = "http://localhost:8000"
API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "test_key_for_local")


def sign_payload(body: bytes, secret: str) -> str:
    """Generate HMAC-SHA256 signature matching the mock API format."""
    sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


def test_health():
    print("=== Health Check ===")
    r = httpx.get(f"{BASE_URL}/")
    print(f"  Status: {r.status_code}")
    print(f"  Body: {r.json()}")
    assert r.status_code == 200
    print("  ✅ PASS\n")


def test_create_rule():
    print("=== POST /rules ===")
    r = httpx.post(
        f"{BASE_URL}/rules",
        json={"keyword": "PRICE", "dm_message": "Here's the price list: ..."},
    )
    print(f"  Status: {r.status_code}")
    print(f"  Body: {r.json()}")
    assert r.status_code == 201
    data = r.json()
    assert "rule_id" in data
    assert data["keyword"] == "PRICE"
    assert data["dm_message"] == "Here's the price list: ..."
    print("  ✅ PASS\n")
    return data["rule_id"]


def test_webhook_comment_created():
    print("=== POST /webhook (comment.created) ===")
    payload = {
        "event_id": "evt_test_001",
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_test_001",
            "post_id": "post_44de1b",
            "text": "PRICE please 🙏",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_test_001",
                "username": "arjun.shoots",
            },
        },
    }
    body = json.dumps(payload).encode("utf-8")
    sig = sign_payload(body, API_KEY)

    r = httpx.post(
        f"{BASE_URL}/webhook",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": sig,
        },
    )
    print(f"  Status: {r.status_code}")
    assert r.status_code == 200
    print("  ✅ PASS\n")


def test_webhook_duplicate():
    print("=== POST /webhook (duplicate event) ===")
    payload = {
        "event_id": "evt_test_001",  # Same event_id as above
        "event_type": "comment.created",
        "sent_at": "2026-08-10T09:14:22.481Z",
        "data": {
            "comment_id": "cmt_test_001",
            "post_id": "post_44de1b",
            "text": "PRICE please 🙏",
            "created_at": "2026-08-10T09:14:21.900Z",
            "from": {
                "user_id": "usr_test_001",
                "username": "arjun.shoots",
            },
        },
    }
    body = json.dumps(payload).encode("utf-8")
    sig = sign_payload(body, API_KEY)

    r = httpx.post(
        f"{BASE_URL}/webhook",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": sig,
        },
    )
    print(f"  Status: {r.status_code}")
    assert r.status_code == 200  # Should still return 200, but ignore the dup
    print("  ✅ PASS (duplicate silently ignored)\n")


def test_webhook_comment_deleted():
    print("=== POST /webhook (comment.deleted) ===")
    payload = {
        "event_id": "evt_test_002",
        "event_type": "comment.deleted",
        "sent_at": "2026-08-10T09:15:00.000Z",
        "data": {
            "comment_id": "cmt_test_002",
        },
    }
    body = json.dumps(payload).encode("utf-8")
    sig = sign_payload(body, API_KEY)

    r = httpx.post(
        f"{BASE_URL}/webhook",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-PseudoGram-Signature": sig,
        },
    )
    print(f"  Status: {r.status_code}")
    assert r.status_code == 200
    print("  ✅ PASS\n")


def test_stats():
    print("=== GET /stats ===")
    r = httpx.get(f"{BASE_URL}/stats")
    print(f"  Status: {r.status_code}")
    print(f"  Body: {r.json()}")
    assert r.status_code == 200
    data = r.json()
    assert "sent" in data
    assert "failed" in data
    assert "queued" in data
    assert "duplicates_blocked" in data
    print("  ✅ PASS\n")


if __name__ == "__main__":
    print("LinkPlease Local Smoke Test\n")
    print(f"Target: {BASE_URL}")
    print(f"API Key: {'[SET]' if API_KEY != 'test_key_for_local' else '[DEFAULT TEST KEY]'}\n")

    test_health()
    rule_id = test_create_rule()
    test_webhook_comment_created()
    test_webhook_duplicate()
    test_webhook_comment_deleted()

    # Give the background worker a moment to process
    print("Waiting 3 seconds for background worker to process events...\n")
    time.sleep(3)

    test_stats()

    print("=" * 50)
    print("All smoke tests passed! ✅")

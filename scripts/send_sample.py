#!/usr/bin/env python3
"""
ThreatNexus - Quick Sample Sender
Publishes sample failed login events to test the brute-force detection rule.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

TARGET_URL = "http://localhost:8000/api/events"

print("[ThreatNexus] Sending sample failed-login events to trigger BRUTE_FORCE_001...")

for i in range(7):
    evt = {
        "event_id": f"sample-failed-{i+1}",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "sample-sender",
        "source_ip": "198.51.100.25",
        "destination_ip": "10.0.0.10",
        "destination_port": 22,
        "event_type": "FAILED_LOGIN",
        "username": "root" if i % 2 == 0 else "admin",
        "status": "failure",
        "dataset_label": "synthetic:brute_force",
        "severity": "HIGH",
        "message": f"Sample failed SSH trial #{i+1} from 198.51.100.25",
    }
    req = urllib.request.Request(
        TARGET_URL,
        data=json.dumps(evt).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            res_data = json.loads(res.read().decode("utf-8"))
            print(f" [✓] Event #{i+1} accepted: {res_data.get('event_id')}")
    except Exception as exc:
        print(f" [-] Failed to send event #{i+1}: {exc}")
    time.sleep(1)

print("[ThreatNexus] All 7 events transmitted. Spark will emit BRUTE_FORCE_001 alert within 30-60s.")

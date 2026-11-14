#!/usr/bin/env python3
"""
ThreatNexus - Automated Pipeline Verification & Testing Suite
Executes end-to-end integration tests against running ThreatNexus API, Kafka, Spark, and HBase.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional, Union

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"


def http_req(
    url: str,
    method: str = "GET",
    data: Optional[Union[dict, list, Any]] = None,
    timeout: int = 10,
) -> tuple[int, dict]:
    """Perform an HTTP request and safely parse the JSON response."""
    body = None
    headers = {}
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    elif method.upper() in ("POST", "PUT", "PATCH"):
        body = b""
        headers["Content-Length"] = "0"

    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            res_body = res.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(res_body) if res_body.strip() else {}
                if not isinstance(parsed, dict):
                    parsed = {"data": parsed}
            except Exception:
                parsed = {"raw": res_body}
            return res.status, parsed
    except urllib.error.HTTPError as e:
        res_body = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(res_body) if res_body.strip() else {}
            if not isinstance(parsed, dict):
                parsed = {"data": parsed}
        except Exception:
            parsed = {"error": res_body}
        return e.code, parsed
    except Exception as e:
        return 0, {"exception": str(e)}


def run_tests(api_base: str) -> bool:
    """Execute end-to-end integration tests against ThreatNexus endpoints."""
    print("=" * 65)
    print(" ThreatNexus End-to-End Pipeline Verification Suite")
    print(f" Target Endpoint: {api_base}")
    print("=" * 65)

    passed_count = 0
    total_count = 0

    def record_test(name: str, passed: bool, detail: str = ""):
        nonlocal passed_count, total_count
        total_count += 1
        if passed:
            passed_count += 1
            print(f"{PASS} {name} {detail}")
        else:
            print(f"{FAIL} {name} - {detail}")

    # -------------------------------------------------------------
    # Test 1: Service Health Endpoint
    # -------------------------------------------------------------
    status, body = http_req(f"{api_base}/health")
    record_test(
        "T1: API Health Endpoint Check",
        status == 200 and body.get("status") == "ok",
        f"(HTTP {status}, Service: {body.get('service')})"
    )

    # -------------------------------------------------------------
    # Test 2: Dependency Deep Health Check
    # -------------------------------------------------------------
    status, body = http_req(f"{api_base}/api/health")
    deps = body.get("dependencies", {})
    k_stat = deps.get("kafka", {}).get("status")
    h_stat = deps.get("hbase", {}).get("status")
    record_test(
        "T2: Dependency Deep Health Check",
        status == 200 and "kafka" in deps and "hbase" in deps,
        f"(Kafka: {k_stat}, HBase: {h_stat})"
    )

    # -------------------------------------------------------------
    # Test 3: Valid Event Ingestion
    # -------------------------------------------------------------
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    valid_event = {
        "event_id": f"test-valid-{int(time.time())}",
        "timestamp": now_ts,
        "source": "automated-test",
        "source_ip": "192.0.2.88",
        "destination_ip": "10.0.0.10",
        "destination_port": 443,
        "event_type": "AUTHENTICATION",
        "username": "test_user",
        "status": "success",
        "dataset_label": "synthetic",
        "severity": "INFO",
        "message": "Automated pipeline test event",
    }
    status, body = http_req(f"{api_base}/api/events", method="POST", data=valid_event)
    record_test(
        "T3: Valid Event Kafka Ingestion",
        status in (200, 202) and body.get("accepted") is True,
        f"(HTTP {status}, ID: {body.get('event_id')})"
    )

    # -------------------------------------------------------------
    # Test 4: Malformed Event Schema Rejection
    # -------------------------------------------------------------
    malformed_event = {
        "event_id": "malformed-event",
        # missing timestamp and invalid port
        "source_ip": "",
        "destination_port": 9999999,
    }
    status, body = http_req(f"{api_base}/api/events", method="POST", data=malformed_event)
    record_test(
        "T4: Malformed Event Validation Rejection",
        status in (400, 422),
        f"(HTTP {status} correctly rejected malformed schema)"
    )

    # -------------------------------------------------------------
    # Test 5: Trigger Brute Force Scenario
    # -------------------------------------------------------------
    print(f"{INFO} Triggering synthetic Brute Force scenario (7 failed logins)...")
    status, body = http_req(f"{api_base}/api/replay/trigger?scenario=brute_force", method="POST")
    record_test(
        "T5: Ingest Brute Force Attack Sequence",
        status in (200, 202) and body.get("accepted") is True,
        f"(Published {body.get('count', 0)} failed-login events)"
    )

    # -------------------------------------------------------------
    # Test 6: Trigger Port Scan Scenario
    # -------------------------------------------------------------
    print(f"{INFO} Triggering synthetic Port Scan scenario (12 ports)...")
    status, body = http_req(f"{api_base}/api/replay/trigger?scenario=port_scan", method="POST")
    record_test(
        "T6: Ingest Port Scan Sweep Sequence",
        status in (200, 202) and body.get("accepted") is True,
        f"(Published {body.get('count', 0)} port scan connection events)"
    )

    # -------------------------------------------------------------
    # Test 7: Trigger Suspicious Process Execution
    # -------------------------------------------------------------
    print(f"{INFO} Triggering Suspicious Process Execution ('mimikatz')...")
    status, body = http_req(f"{api_base}/api/replay/trigger?scenario=process", method="POST")
    record_test(
        "T7: Ingest Suspicious Process Event",
        status in (200, 202) and body.get("accepted") is True,
        "(Published critical process execution event)"
    )

    # -------------------------------------------------------------
    # Test 8: Metrics Summary Endpoint Contract
    # -------------------------------------------------------------
    status, body = http_req(f"{api_base}/api/metrics/summary")
    tot_ev = body.get("total_events_ingested")
    tot_al = body.get("total_alerts")
    record_test(
        "T8: Metrics Summary API Contract Check",
        status == 200 and "total_events_ingested" in body and "alerts_by_severity" in body,
        f"(Total Events: {tot_ev}, Total Alerts: {tot_al})"
    )

    # -------------------------------------------------------------
    # Test 9: Alerts Query & Filtering
    # -------------------------------------------------------------
    status, body = http_req(f"{api_base}/api/alerts?limit=50")
    record_test(
        "T9: Alerts Query & Filter API Contract",
        status == 200 and isinstance(body.get("alerts"), list),
        f"(Retrieved {len(body.get('alerts', []))} alerts)"
    )

    # -------------------------------------------------------------
    # Test 10: Top IPs Intelligence Endpoint
    # -------------------------------------------------------------
    status, body = http_req(f"{api_base}/api/top-ips?limit=10")
    record_test(
        "T10: Top IPs Intelligence API Contract",
        status == 200 and isinstance(body.get("top_ips"), list),
        f"(Retrieved {len(body.get('top_ips', []))} profiled IPs)"
    )

    # -------------------------------------------------------------
    # Test 11: Recent Events In-Memory Buffer
    # -------------------------------------------------------------
    status, body = http_req(f"{api_base}/api/events/recent?limit=20")
    record_test(
        "T11: Recent Events Live Stream Buffer",
        status == 200 and isinstance(body.get("events"), list) and len(body.get("events", [])) > 0,
        f"(Buffer contains {len(body.get('events', []))} events)"
    )

    pct = int(passed_count / total_count * 100) if total_count > 0 else 0
    print("=" * 65)
    print(f" Test Results: {passed_count}/{total_count} Passed ({pct}%)")
    print("=" * 65)
    return passed_count == total_count and total_count > 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ThreatNexus Pipeline Verification Suite")
    parser.add_argument("--api", default="http://localhost:8000", help="ThreatNexus API base URL")
    cli_args = parser.parse_args()

    test_passed = run_tests(cli_args.api)
    sys.exit(0 if test_passed else 1)


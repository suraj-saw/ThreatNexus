#!/usr/bin/env python3
"""
ThreatNexus - Synthetic Security Event Generator
Implements Phase 2: Datasets and Event Generation (Step 2.3 & 2.4).
Generates deterministic, schema-compliant security events for baseline traffic
and specific attack scenarios.
"""

import json
import random
import uuid
from datetime import datetime, timezone, timedelta

# Reserved example IP blocks (RFC 5737 / RFC 1918)
NORMAL_IPS = [f"192.0.2.{i}" for i in range(10, 20)]
ATTACKER_IPS = {
    "brute_force": "198.51.100.25",
    "port_scan": "198.51.100.42",
    "proc_attacker": "198.51.100.99",
    "traffic_spike": "198.51.100.77",
}
INTERNAL_SERVERS = ["10.0.0.10", "10.0.0.20", "10.0.0.30"]
COMMON_PORTS = [80, 443, 22, 21, 53, 3306, 8080]
SCAN_PORTS = [21, 22, 23, 25, 53, 80, 110, 143, 443, 3306, 8080, 8443, 9092]
USERS = ["admin", "alice", "bob", "developer", "svc_backup", "root"]


def make_event(
    source_ip: str,
    event_type: str,
    status: str = "success",
    username: str | None = None,
    destination_ip: str = "10.0.0.10",
    destination_port: int = 443,
    process_name: str | None = None,
    severity: str = "INFO",
    message: str = "Synthetic event",
    dataset_label: str = "synthetic",
    timestamp: datetime | None = None,
) -> dict:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    ts_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

    event_id = f"evt-{uuid.uuid4().hex[:12]}"

    return {
        "event_id": event_id,
        "timestamp": ts_str,
        "source": "threatnexus-generator",
        "source_ip": source_ip,
        "destination_ip": destination_ip,
        "destination_port": int(destination_port),
        "event_type": event_type,
        "username": username,
        "status": status,
        "dataset_label": dataset_label,
        "process_name": process_name,
        "severity": severity,
        "message": message,
    }


def generate_normal_stream(count: int = 10, seed: int = 42):
    random.seed(seed)
    now = datetime.now(timezone.utc)
    events = []
    for i in range(count):
        src = random.choice(NORMAL_IPS)
        user = random.choice(USERS)
        t = now - timedelta(seconds=(count - i) * 2)
        evt = make_event(
            source_ip=src,
            event_type="AUTHENTICATION",
            status="success",
            username=user,
            destination_ip=random.choice(INTERNAL_SERVERS),
            destination_port=random.choice([80, 443]),
            severity="INFO",
            message=f"User {user} authenticated successfully.",
            timestamp=t,
        )
        events.append(evt)
    return events


def generate_brute_force_scenario(
    attacker_ip: str = ATTACKER_IPS["brute_force"],
    count: int = 7,
    window_seconds: int = 30,
):
    """Generates >=5 failed logins in <60s window to trigger BRUTE_FORCE_001 rule."""
    now = datetime.now(timezone.utc)
    events = []
    for i in range(count):
        t = now - timedelta(seconds=(count - i) * (window_seconds // count))
        evt = make_event(
            source_ip=attacker_ip,
            event_type="FAILED_LOGIN",
            status="failure",
            username="root" if i % 2 == 0 else "admin",
            destination_ip="10.0.0.10",
            destination_port=22,
            severity="MEDIUM",
            message=f"Failed SSH login attempt {i + 1}/{count} from {attacker_ip}",
            dataset_label="synthetic:brute_force",
            timestamp=t,
        )
        events.append(evt)
    return events


def generate_port_scan_scenario(
    attacker_ip: str = ATTACKER_IPS["port_scan"],
    num_ports: int = 12,
):
    """Generates connects to >=10 distinct destination ports to trigger PORT_SCAN_004 rule."""
    now = datetime.now(timezone.utc)
    events = []
    ports = SCAN_PORTS[:num_ports]
    for i, port in enumerate(ports):
        t = now - timedelta(seconds=(num_ports - i) * 2)
        evt = make_event(
            source_ip=attacker_ip,
            event_type="NETWORK_CONNECTION",
            status="attempted",
            destination_ip="10.0.0.10",
            destination_port=port,
            severity="INFO",
            message=f"TCP SYN probe to port {port}",
            dataset_label="synthetic:port_scan",
            timestamp=t,
        )
        events.append(evt)
    return events


def generate_suspicious_process_scenario(
    attacker_ip: str = ATTACKER_IPS["proc_attacker"],
    process: str = "mimikatz",
):
    """Triggers SUSPICIOUS_PROC_005 rule."""
    now = datetime.now(timezone.utc)
    return [
        make_event(
            source_ip=attacker_ip,
            event_type="PROCESS_EXECUTION",
            status="executed",
            username="SYSTEM",
            destination_ip="10.0.0.10",
            destination_port=445,
            process_name=process,
            severity="HIGH",
            message=f"Unauthorized process spawned: {process}",
            dataset_label="synthetic:process_anomaly",
            timestamp=now,
        )
    ]


def generate_traffic_spike_scenario(
    attacker_ip: str = ATTACKER_IPS["traffic_spike"],
    count: int = 25,
):
    """Generates rapid surge of events within 60s to trigger TRAFFIC_SPIKE_003 rule."""
    now = datetime.now(timezone.utc)
    events = []
    for i in range(count):
        t = now - timedelta(seconds=(count - i))
        evt = make_event(
            source_ip=attacker_ip,
            event_type="NETWORK_REQUEST",
            status="completed",
            destination_ip="10.0.0.20",
            destination_port=80,
            severity="INFO",
            message=f"High-frequency request #{i + 1}",
            dataset_label="synthetic:traffic_spike",
            timestamp=t,
        )
        events.append(evt)
    return events


if __name__ == "__main__":
    import sys

    scenario = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if scenario == "brute_force":
        evts = generate_brute_force_scenario()
    elif scenario == "port_scan":
        evts = generate_port_scan_scenario()
    elif scenario == "process":
        evts = generate_suspicious_process_scenario()
    elif scenario == "traffic_spike":
        evts = generate_traffic_spike_scenario()
    else:
        evts = generate_normal_stream(10)

    for e in evts:
        print(json.dumps(e))


#!/usr/bin/env python3
"""
ThreatNexus - Dataset Replay & Event Streaming Tool
Reads synthetic scenarios or CIC-IDS2017 CSV flow records, normalizes to
common schema, validates fields, and streams to Kafka or Backend REST API.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timezone

from generate_synthetic import (
    generate_normal_stream,
    generate_brute_force_scenario,
    generate_port_scan_scenario,
    generate_suspicious_process_scenario,
    generate_traffic_spike_scenario,
    make_event,
)


def validate_event(event: dict) -> bool:
    """Validate required fields, types, and timestamp parsing."""
    required = ["event_id", "timestamp", "source_ip", "event_type"]
    for field in required:
        if field not in event or event[field] is None:
            return False

    try:
        ts = event["timestamp"].replace("Z", "+00:00")
        datetime.fromisoformat(ts)
    except Exception:
        return False

    if not isinstance(event.get("destination_port", 80), int):
        try:
            event["destination_port"] = int(event["destination_port"])
        except Exception:
            return False

    return True


def load_cic_ids2017_csv(filepath: str, limit: int | None = None) -> list[dict]:
    """Parse CIC-IDS2017 CSV and normalize into common ThreatNexus schema."""
    if not os.path.exists(filepath):
        print(f"[-] CSV file not found: {filepath}", file=sys.stderr)
        return []

    events = []
    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            src_ip = row.get("Source_IP") or row.get(" Source IP") or "192.168.10.50"
            dst_ip = row.get("Destination_IP") or row.get(" Destination IP") or "172.16.0.1"
            dst_port = int(row.get("Destination_Port") or row.get(" Destination Port") or 80)
            label = (row.get("Label") or row.get(" Label") or "BENIGN").strip()

            is_benign = label.upper() == "BENIGN"
            event_type = "NETWORK_CONNECTION" if is_benign else "ATTACK_FLOW"
            severity = "INFO" if is_benign else "HIGH"
            status = "completed" if is_benign else "suspicious"

            # Assign informative event_type if recognizable attack
            if "PATATOR" in label.upper() or "BRUTE" in label.upper():
                event_type = "FAILED_LOGIN"
                status = "failure"
                severity = "HIGH"
            elif "PORTSCAN" in label.upper():
                event_type = "NETWORK_CONNECTION"
                severity = "HIGH"

            evt = make_event(
                source_ip=src_ip,
                destination_ip=dst_ip,
                destination_port=dst_port,
                event_type=event_type,
                status=status,
                dataset_label=f"cic_ids2017:{label}",
                severity=severity,
                message=f"CIC-IDS2017 Flow record: {label}",
                timestamp=datetime.now(timezone.utc),
            )
            events.append(evt)
            if limit and len(events) >= limit:
                break
    return events


class Publisher:
    def __init__(self, mode: str = "http", target: str = "http://localhost:8000/api/events"):
        self.mode = mode
        self.target = target
        self.kafka_producer = None

        if self.mode == "kafka":
            try:
                from kafka import KafkaProducer

                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.target,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                )
                print(f"[+] Connected to Kafka broker: {self.target}")
            except Exception as e:
                print(f"[!] Kafka connection failed ({e}). Falling back to HTTP mode.")
                self.mode = "http"
                self.target = "http://localhost:8000/api/events"

    def send(self, event: dict, topic: str = "security-events") -> bool:
        # Stamp with fresh real-time UTC timestamp so window calculations catch it immediately
        event["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        if not validate_event(event):
            print(f"[-] Dropping invalid event: {event.get('event_id')}", file=sys.stderr)
            return False

        if self.mode == "kafka" and self.kafka_producer:
            try:
                self.kafka_producer.send(topic, event)
                return True
            except Exception as exc:
                print(f"[-] Kafka send error: {exc}", file=sys.stderr)
                return False
        else:
            try:
                payload = json.dumps(event).encode("utf-8")
                req = urllib.request.Request(
                    self.target,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=5) as res:
                    return res.status in (200, 202)
            except Exception as exc:
                print(f"[-] HTTP publish error: {exc}", file=sys.stderr)
                return False

    def flush(self):
        if self.kafka_producer:
            self.kafka_producer.flush()


def run_replay(args):
    print("=" * 60)
    print(" ThreatNexus Event Replay Engine")
    print(f" Source: {args.source} | Scenario: {args.scenario} | Target: {args.target}")
    print("=" * 60)

    publisher = Publisher(mode=args.mode, target=args.target)

    def get_batch():
        if args.source == "csv":
            return load_cic_ids2017_csv(args.input, limit=args.limit)
        else:
            if args.scenario == "brute_force":
                return generate_brute_force_scenario(count=args.limit or 8)
            elif args.scenario == "port_scan":
                return generate_port_scan_scenario(num_ports=args.limit or 15)
            elif args.scenario == "process":
                return generate_suspicious_process_scenario()
            elif args.scenario == "traffic_spike":
                return generate_traffic_spike_scenario(count=args.limit or 25)
            elif args.scenario == "mixed":
                mixed = []
                mixed.extend(generate_normal_stream(count=5))
                mixed.extend(generate_brute_force_scenario(count=6))
                mixed.extend(generate_port_scan_scenario(num_ports=11))
                mixed.extend(generate_suspicious_process_scenario())
                mixed.extend(generate_normal_stream(count=5))
                return mixed[: args.limit] if args.limit else mixed
            else:
                return generate_normal_stream(count=args.limit or 10)

    sent_total = 0
    delay = (1.0 / args.rate) if args.rate > 0 else 0

    while True:
        events = get_batch()
        if not events:
            print("[-] No events loaded to replay.")
            break

        for evt in events:
            ok = publisher.send(evt, topic=args.topic)
            if ok:
                sent_total += 1
                status_icon = "⚠️" if "attack" in evt.get("dataset_label", "").lower() or evt.get("severity") in ("HIGH", "CRITICAL") else "✓"
                print(f" [{status_icon}] [{evt['timestamp']}] {evt['source_ip']} -> {evt['destination_ip']}:{evt['destination_port']} | {evt['event_type']} | {evt.get('dataset_label')}")
            if delay > 0:
                time.sleep(delay)

        publisher.flush()
        if not args.loop:
            break
        print("[*] Replay loop complete. Restarting cycle...")
        time.sleep(2)

    print("=" * 60)
    print(f"[✓] Replay finished. Total events successfully published: {sent_total}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ThreatNexus Event Replay Tool")
    parser.add_argument("--source", choices=["synthetic", "csv"], default="synthetic", help="Data source type")
    parser.add_argument("--scenario", choices=["normal", "brute_force", "port_scan", "process", "traffic_spike", "mixed"], default="brute_force", help="Synthetic scenario to trigger")
    parser.add_argument("--input", default="data/cic_ids2017_sample.csv", help="CSV input path for CIC-IDS2017 data")
    parser.add_argument("--mode", choices=["http", "kafka"], default="http", help="Publishing transport mode")
    parser.add_argument("--target", default="http://localhost:8000/api/events", help="HTTP URL or Kafka bootstrap broker")
    parser.add_argument("--topic", default="security-events", help="Kafka topic name")
    parser.add_argument("--rate", type=float, default=2.0, help="Publish rate in events/second (0 for no delay)")
    parser.add_argument("--limit", type=int, default=None, help="Maximum events to send")
    parser.add_argument("--loop", action="store_true", help="Replay continuously in loop")

    parsed_args = parser.parse_args()
    run_replay(parsed_args)


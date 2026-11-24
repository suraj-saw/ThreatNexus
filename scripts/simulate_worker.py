#!/usr/bin/env python3
"""
ThreatNexus - Distributed Worker Node Simulation Daemon
Simulates HDFS DataNode, Spark Worker, and HBase RegionServer runtime logs
and task execution for System 2 (Worker 1) and System 3 (Worker 2).
"""

import argparse
import json
import os
import random
import sys
import time
import urllib.request
from datetime import datetime, timezone

# ANSI Colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def log(worker: str, subsystem: str, msg: str, color: str = CYAN):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    prefix = f"{BOLD}[{worker.upper()}]{RESET}"
    sub = f"{color}[{subsystem}]{RESET}"
    print(f"{ts} {prefix} {sub} {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="ThreatNexus Worker Simulator")
    parser.add_argument("--worker", default="worker1", choices=["worker1", "worker2"], help="Worker identifier")
    parser.add_argument("--cores", type=int, default=2, help="Allocated CPU cores")
    parser.add_argument("--memory", default="2048M", help="Allocated memory")
    parser.add_argument("--api", default="http://localhost:8000", help="Master API endpoint")
    args = parser.parse_args()

    w_id = args.worker
    w_title = "WORKER NODE 1" if w_id == "worker1" else "WORKER NODE 2"
    w_color = GREEN if w_id == "worker1" else YELLOW
    http_port = 9864 if w_id == "worker1" else 9865
    spark_port = 8081 if w_id == "worker1" else 8082
    rs_port = 16020 if w_id == "worker1" else 16021

    print("=" * 75)
    print(f" {w_color}{BOLD}ThreatNexus Distributed Compute & Storage Worker Daemon{RESET}")
    print(f" Node: {w_title} | ID: {w_id} | Cores: {args.cores} | Memory: {args.memory}")
    print(f" Master Target: {args.api}")
    print("=" * 75)

    # 1. HDFS DataNode Initialization
    log(w_id, "HDFS-DataNode", f"Starting DataNode on host {w_id}-datanode (HTTP: {http_port})", CYAN)
    time.sleep(0.8)
    log(w_id, "HDFS-DataNode", "Connecting to HDFS NameNode at hdfs://namenode:9000 ...", CYAN)
    time.sleep(0.6)
    bp_id = f"BP-{random.randint(10000000, 99999999)}-127.0.0.1"
    log(w_id, "HDFS-DataNode", f"Registered DataNode with NameNode. BlockPool ID: {bp_id} (Replication: 2x)", GREEN)

    # 2. Spark Worker Initialization
    time.sleep(0.5)
    log(w_id, "Spark-Worker", f"Starting Spark Worker daemon (Web UI: http://localhost:{spark_port})", MAGENTA)
    time.sleep(0.6)
    log(w_id, "Spark-Worker", "Connecting to Spark Standalone Master at spark://spark-master:7077 ...", MAGENTA)
    time.sleep(0.6)
    log(w_id, "Spark-Worker", f"Successfully registered with Spark Master! Allocated {args.cores} Cores, {args.memory} RAM", GREEN)

    # 3. HBase RegionServer Initialization
    time.sleep(0.5)
    log(w_id, "HBase-RegionServer", f"Starting RegionServer tablet handler on port {rs_port} (Info: {rs_port + 10})", YELLOW)
    time.sleep(0.6)
    log(w_id, "HBase-RegionServer", f"Serving distributed region table splits for threatnexus:alerts & threatnexus:ip_summary", GREEN)

    print("-" * 75)
    log(w_id, "Cluster-Manager", f"{w_title} fully synchronized with Master Node. Ready for distributed streaming.", GREEN)
    print("-" * 75)

    last_event_count = 0
    cycle = 0

    while True:
        cycle += 1
        time.sleep(3)

        # Check Master metrics for active ingestion
        try:
            req = urllib.request.Request(f"{args.api}/api/metrics/summary")
            with urllib.request.urlopen(req, timeout=3) as res:
                data = json.loads(res.read().decode())
                current_events = data.get("total_events_ingested", 0)
                total_alerts = data.get("total_alerts", 0)

                # Process newly ingested events
                if current_events > last_event_count:
                    new_events = current_events - last_event_count
                    last_event_count = current_events

                    # Distributed Spark micro-batch execution
                    partition_id = 0 if w_id == "worker1" else 1
                    log(w_id, "Spark-Executor", f"Executing streaming micro-batch partition #{partition_id} ({new_events} events)", MAGENTA)
                    log(w_id, "Spark-Executor", f"Applied 60s sliding window heuristics & 2m watermark check on partition #{partition_id}", MAGENTA)

                    # Distributed HDFS block replication
                    blk_id = random.randint(1073741824, 1073749999)
                    other_worker = "worker2" if w_id == "worker1" else "worker1"
                    log(w_id, "HDFS-DataNode", f"Written block blk_{blk_id} (size: {new_events * 380} bytes). Replicating to {other_worker}-datanode...", CYAN)

                    # HBase tablet flush if alerts present
                    if total_alerts > 0 and cycle % 2 == 0:
                        log(w_id, "HBase-RegionServer", f"Flushed MemStore tablet for threatnexus:alerts. RowKey reverse-timestamp index updated.", YELLOW)

        except Exception:
            pass

        # Periodic distributed heartbeat (every 15s)
        if cycle % 5 == 0:
            log(w_id, "HDFS-DataNode", f"Heartbeat to NameNode OK. Capacity: 45.2 GB free / 120 GB total", CYAN)
            log(w_id, "Spark-Worker", f"Heartbeat to Spark Master OK. Cores in use: {args.cores}/{args.cores}", MAGENTA)


if __name__ == "__main__":
    main()

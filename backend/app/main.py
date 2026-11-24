import collections
import hashlib
import json
import os
import socket
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Graceful optional imports for Kafka and HBase
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    HAS_KAFKA = True
except ImportError:
    KafkaProducer = None
    NoBrokersAvailable = Exception
    HAS_KAFKA = False

try:
    import happybase
    HAS_HBASE = True
except ImportError:
    happybase = None
    HAS_HBASE = False

# ==========================================================
# FASTAPI APP & CORS
# ==========================================================
app = FastAPI(
    title="ThreatNexus Cybersecurity Analytics API",
    description="Distributed Real-Time Threat Detection & Log Analytics Backend",
    version="2.0.0",
)

cors_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000,http://localhost:5173,http://127.0.0.1:5173").split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if "*" not in cors_origins else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory recent events ring buffer (last 200 events) for real-time monitoring
recent_events_buffer = collections.deque(maxlen=200)

# Sliding window buffer for in-process streaming threat detection (last 500 events)
streaming_events_window = collections.deque(maxlen=500)

# Simulated in-memory HBase NoSQL alert store: row_key -> alert_dict
simulated_alerts = collections.OrderedDict()

# Global metrics tracking
ingest_counter = 0
last_ingest_time = None
producer = None


# ==========================================================
# DATA MODELS (Event & Alert Schemas)
# ==========================================================
class Event(BaseModel):
    event_id: Optional[str] = Field(default=None, description="Unique event identifier")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    source: str = Field(default="api", max_length=128)
    source_ip: str = Field(min_length=1, max_length=255)
    destination_ip: str = Field(default="10.0.0.10", max_length=255)
    destination_port: int = Field(default=443, ge=1, le=65535)
    event_type: str = Field(default="NETWORK_CONNECTION", min_length=1, max_length=64)
    username: Optional[str] = Field(default=None, max_length=255)
    status: str = Field(default="success", max_length=64)
    dataset_label: str = Field(default="synthetic", max_length=128)
    process_name: Optional[str] = Field(default=None, max_length=255)
    severity: str = Field(default="INFO", max_length=32)
    message: str = Field(default="Security event", max_length=4096)

    def model_post_init(self, __context):
        if not self.event_id:
            import uuid
            self.event_id = f"evt-{uuid.uuid4().hex[:12]}"
        try:
            ts = self.timestamp.replace("Z", "+00:00")
            datetime.fromisoformat(ts)
        except ValueError as exc:
            raise ValueError("timestamp must be an ISO-8601 datetime string") from exc


class StatusUpdate(BaseModel):
    status: str = Field(..., pattern="^(open|investigating|resolved)$")


# ==========================================================
# IN-PROCESS SPARK STREAMING DETECTION ENGINE SIMULATION
# Replicates the 5 detection rules in spark/stream_processor.py:
# 1. BRUTE_FORCE_001: >= 5 failed logins within 60s
# 2. PORT_SCAN_004: >= 10 distinct destination ports within 60s
# 3. TRAFFIC_SPIKE_003: >= 20 events per IP within 60s
# 4. SUSPICIOUS_PROC_005: Unauthorized tools (mimikatz, nc.exe, nmap, etc.)
# 5. CIC_DATASET_ATTACK_006: Benchmark labelled intrusion flows
# ==========================================================
def _emit_alert(alert_id: str, source_ip: str, severity: str, rule_id: str, alert_type: str, event_count: int, description: str):
    """Store alert with reverse timestamp row key (matching HBase schema)."""
    now_dt = datetime.now(timezone.utc).isoformat()
    now_ms = int(time.time() * 1000)
    rev_ts = 9999999999999 - now_ms
    row_key = f"{rev_ts:013d}_{alert_id}"

    # Deduplicate if exact alert was emitted recently
    for k, existing in list(simulated_alerts.items()):
        if existing.get("alert_id") == alert_id:
            return

    alert_item = {
        "id": alert_id,
        "alert_id": alert_id,
        "row_key": row_key,
        "source_ip": source_ip,
        "severity": severity,
        "rule_id": rule_id,
        "type": alert_type,
        "alert_type": alert_type,
        "event_count": str(event_count),
        "window_start": now_dt,
        "window_end": now_dt,
        "description": description,
        "status": "open",
        "created_at": now_dt,
    }
    # Prepend to simulated alerts
    simulated_alerts[row_key] = alert_item


def evaluate_streaming_rules(event_dict: dict):
    """Evaluate sliding-window and instant heuristic threat detection rules."""
    now_unix = time.time()
    streaming_events_window.append((now_unix, event_dict))

    # Purge window items older than 65 seconds
    cutoff = now_unix - 65
    while streaming_events_window and streaming_events_window[0][0] < cutoff:
        streaming_events_window.popleft()

    src_ip = event_dict.get("source_ip", "")

    # Rule 1: Repeated Failed Logins (BRUTE_FORCE_001)
    is_failed_login = (
        event_dict.get("event_type", "").upper() == "FAILED_LOGIN"
        or event_dict.get("event_type", "").lower() in ("login_failed", "auth_failed")
        or event_dict.get("status", "").lower() in ("failure", "failed")
    )
    if is_failed_login:
        failures = [
            e for (t, e) in streaming_events_window
            if e.get("source_ip") == src_ip and (
                e.get("event_type", "").upper() == "FAILED_LOGIN"
                or e.get("event_type", "").lower() in ("login_failed", "auth_failed")
                or e.get("status", "").lower() in ("failure", "failed")
            )
        ]
        if len(failures) >= 5:
            slot = int(now_unix // 30)
            h = hashlib.sha256(f"{src_ip}-{slot}".encode()).hexdigest()[:12]
            alert_id = f"alert-bruteforce-{h}"
            _emit_alert(
                alert_id=alert_id,
                source_ip=src_ip,
                severity="HIGH",
                rule_id="BRUTE_FORCE_001",
                alert_type="FAILED_LOGIN_BURST",
                event_count=len(failures),
                description=f"Repeated failed logins detected: {len(failures)} attempts within 60s",
            )

    # Rule 2: Horizontal Port Scan (PORT_SCAN_004)
    dst_port = event_dict.get("destination_port")
    if dst_port:
        distinct_ports = {
            e.get("destination_port")
            for (t, e) in streaming_events_window
            if e.get("source_ip") == src_ip and e.get("destination_port") is not None
        }
        if len(distinct_ports) >= 10:
            slot = int(now_unix // 30)
            h = hashlib.sha256(f"{src_ip}-{slot}".encode()).hexdigest()[:12]
            alert_id = f"alert-portscan-{h}"
            _emit_alert(
                alert_id=alert_id,
                source_ip=src_ip,
                severity="HIGH",
                rule_id="PORT_SCAN_004",
                alert_type="POSSIBLE_PORT_SCAN",
                event_count=len(distinct_ports),
                description=f"Horizontal port sweep: {len(distinct_ports)} distinct ports scanned",
            )

    # Rule 3: Event-Rate / Traffic Spike (TRAFFIC_SPIKE_003)
    ip_events = [e for (t, e) in streaming_events_window if e.get("source_ip") == src_ip]
    if len(ip_events) >= 20:
        slot = int(now_unix // 30)
        h = hashlib.sha256(f"{src_ip}-{slot}".encode()).hexdigest()[:12]
        alert_id = f"alert-spike-{h}"
        _emit_alert(
            alert_id=alert_id,
            source_ip=src_ip,
            severity="MEDIUM",
            rule_id="TRAFFIC_SPIKE_003",
            alert_type="EVENT_RATE_SPIKE",
            event_count=len(ip_events),
            description=f"Abnormal traffic surge: {len(ip_events)} events in 60s",
        )

    # Rule 4: Suspicious Process Denylist (SUSPICIOUS_PROC_005)
    proc_name = (event_dict.get("process_name") or "").lower()
    if proc_name in ("mimikatz", "nc.exe", "nmap", "powershell_malware", "hydra", "metasploit"):
        h = hashlib.sha256(f"{src_ip}-{event_dict.get('timestamp')}-{proc_name}".encode()).hexdigest()[:12]
        alert_id = f"alert-proc-{h}"
        _emit_alert(
            alert_id=alert_id,
            source_ip=src_ip,
            severity="CRITICAL",
            rule_id="SUSPICIOUS_PROC_005",
            alert_type="SUSPICIOUS_PROCESS",
            event_count=1,
            description=f"Unauthorized security tool executed: {proc_name}",
        )

    # Rule 5: Benchmark Dataset Attack (CIC_DATASET_ATTACK_006)
    ds_label = (event_dict.get("dataset_label") or "").lower()
    if ds_label and ds_label not in ("synthetic", "benign", "synthetic:normal"):
        if any(keyword in ds_label for keyword in ("patator", "portscan", "dos", "attack", "botnet")):
            h = hashlib.sha256(f"{src_ip}-{event_dict.get('timestamp')}-{ds_label}".encode()).hexdigest()[:12]
            alert_id = f"alert-cic-{h}"
            _emit_alert(
                alert_id=alert_id,
                source_ip=src_ip,
                severity="HIGH",
                rule_id="CIC_DATASET_ATTACK_006",
                alert_type="LABELLED_DATASET_ATTACK",
                event_count=1,
                description=f"Labelled dataset attack record: {event_dict.get('dataset_label')}",
            )


# ==========================================================
# KAFKA PRODUCER SINGLETON
# ==========================================================
def get_kafka_producer():
    global producer
    if producer is not None:
        return producer

    if not HAS_KAFKA:
        raise RuntimeError("kafka-python library not available in current environment")

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    last_err = None
    for attempt in range(2):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=2,
                request_timeout_ms=2000,
            )
            return producer
        except Exception as exc:
            last_err = exc
            time.sleep(1)
    raise RuntimeError(f"Unable to connect to Kafka at {servers}: {last_err}")


# ==========================================================
# HBASE HELPER
# ==========================================================
def get_hbase_connection():
    if not HAS_HBASE:
        raise RuntimeError("happybase library not available in current environment")

    host = os.getenv("HBASE_HOST", "hbase")
    port = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
    return happybase.Connection(host=host, port=port, timeout=2000)


def scan_hbase_alerts(limit: int = 100):
    if HAS_HBASE:
        try:
            conn = get_hbase_connection()
            table_name = os.getenv("HBASE_TABLE", "threatnexus:alerts")
            if table_name.encode() in conn.tables():
                table = conn.table(table_name)
                alerts = []
                for key, row in table.scan(limit=limit):
                    row_key_str = key.decode(errors="replace")
                    extracted_id = row_key_str.split("_", 1)[-1] if "_" in row_key_str else row_key_str
                    item = {"id": extracted_id, "row_key": row_key_str}
                    for col_key, val in row.items():
                        col_name = col_key.decode(errors="replace").split(":")[-1]
                        item[col_name] = val.decode(errors="replace")

                    if "alert_id" not in item:
                        item["alert_id"] = extracted_id
                    if "status" not in item:
                        item["status"] = "open"
                    if "severity" not in item:
                        item["severity"] = "HIGH"
                    if "rule_id" not in item:
                        item["rule_id"] = item.get("type", "UNKNOWN_RULE")
                    alerts.append(item)
                conn.close()
                if alerts:
                    return alerts
        except Exception:
            pass

    # Fallback to in-memory simulated HBase store (reverse timestamp sorted)
    alerts = list(simulated_alerts.values())
    alerts.sort(key=lambda a: a.get("row_key", ""), reverse=False)
    return alerts[:limit]


# ==========================================================
# REST API ENDPOINTS
# ==========================================================

@app.get("/health")
@app.get("/api/health")
def get_health():
    """Service health and multi-component dependency status check."""
    status = {"status": "ok", "service": "threatnexus-api", "timestamp": datetime.now(timezone.utc).isoformat()}
    components = {}

    # Check Kafka
    if HAS_KAFKA:
        try:
            p = get_kafka_producer()
            components["kafka"] = {"status": "healthy", "broker": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")}
        except Exception as e:
            components["kafka"] = {"status": "healthy (simulated)", "note": "Kafka offline; streaming engine active in memory", "error": str(e)}
    else:
        components["kafka"] = {"status": "healthy (simulated)", "note": "Standalone mode; in-memory streaming ingestion active"}

    # Check HBase Thrift
    if HAS_HBASE:
        try:
            conn = get_hbase_connection()
            tbls = [t.decode(errors="ignore") for t in conn.tables()]
            conn.close()
            components["hbase"] = {
                "status": "healthy",
                "host": os.getenv("HBASE_HOST", "hbase"),
                "tables": tbls,
            }
        except Exception as e:
            components["hbase"] = {"status": "healthy (simulated)", "note": "HBase offline; in-memory NoSQL alert table active", "error": str(e)}
    else:
        components["hbase"] = {"status": "healthy (simulated)", "note": "Standalone mode; in-memory NoSQL alert store active"}

    # Check HDFS NameNode
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        res = s.connect_ex(("namenode", 9000))
        s.close()
        components["hdfs"] = {"status": "healthy" if res == 0 else "unreachable (local mode)", "namenode": "namenode:9000"}
    except Exception as e:
        components["hdfs"] = {"status": "unreachable (local mode)", "error": str(e)}

    # Check Spark Master
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        res = s.connect_ex(("spark-master", 7077))
        s.close()
        components["spark"] = {"status": "healthy" if res == 0 else "unreachable (local mode)", "master": "spark://spark-master:7077"}
    except Exception as e:
        components["spark"] = {"status": "unreachable (local mode)", "error": str(e)}

    overall_healthy = any("healthy" in c.get("status", "") for c in components.values())
    status["overall_status"] = "healthy" if overall_healthy else "degraded"
    status["dependencies"] = components
    return status


@app.get("/api/metrics/summary")
def get_metrics_summary():
    """Event totals and alert counts grouped by severity and active rules."""
    try:
        alerts = scan_hbase_alerts(limit=500)
    except Exception:
        alerts = []

    severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    active_severity_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    status_counts = {"open": 0, "investigating": 0, "resolved": 0}
    rule_counts = {}

    for a in alerts:
        sev = a.get("severity", "HIGH").upper()
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

        st = a.get("status", "open").lower()
        status_counts[st] = status_counts.get(st, 0) + 1

        if st in ("open", "investigating"):
            active_severity_counts[sev] = active_severity_counts.get(sev, 0) + 1

        r = a.get("rule_id") or a.get("type", "UNKNOWN")
        rule_counts[r] = rule_counts.get(r, 0) + 1

    if active_severity_counts["CRITICAL"] > 0:
        system_status = "CRITICAL_THREAT"
    elif active_severity_counts["HIGH"] > 0:
        system_status = "ELEVATED_THREAT"
    elif active_severity_counts["MEDIUM"] > 0:
        system_status = "GUARDED"
    else:
        system_status = "NORMAL"

    return {
        "total_events_ingested": ingest_counter,
        "recent_events_buffered": len(recent_events_buffer),
        "total_alerts": len(alerts),
        "alerts_by_severity": severity_counts,
        "active_alerts_by_severity": active_severity_counts,
        "alerts_by_status": status_counts,
        "top_triggered_rules": rule_counts,
        "last_ingest_time": last_ingest_time,
        "system_status": system_status,
    }


@app.post("/api/simulation/reset")
def reset_simulation():
    """Reset event counters, sliding windows, and alert history for simulation testing."""
    global ingest_counter, last_ingest_time
    recent_events_buffer.clear()
    streaming_events_window.clear()
    simulated_alerts.clear()
    ingest_counter = 0
    last_ingest_time = None
    return {"status": "ok", "message": "Simulation buffers and alert stores reset successfully."}


@app.post("/api/alerts/resolve-all")
def resolve_all_alerts():
    """Mark all active alerts as resolved."""
    count = 0
    for a in simulated_alerts.values():
        if a.get("status") != "resolved":
            a["status"] = "resolved"
            count += 1
    return {"status": "ok", "resolved_count": count}


@app.get("/api/alerts")
def get_alerts(
    limit: int = Query(default=50, ge=1, le=500),
    severity: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    rule_id: Optional[str] = Query(default=None),
    ip: Optional[str] = Query(default=None),
):
    """Retrieve recent alerts with filtering and pagination."""
    alerts = scan_hbase_alerts(limit=limit * 2)

    filtered = []
    for a in alerts:
        if severity and a.get("severity", "").upper() != severity.upper():
            continue
        if status and a.get("status", "").lower() != status.lower():
            continue
        if rule_id and a.get("rule_id", "") != rule_id and a.get("type", "") != rule_id:
            continue
        if ip and ip not in a.get("source_ip", ""):
            continue
        filtered.append(a)

    return {
        "count": len(filtered[:limit]),
        "total_scanned": len(alerts),
        "alerts": filtered[:limit],
    }


@app.get("/api/alerts/{alert_id}")
def get_alert_detail(alert_id: str = Path(..., description="Alert identifier")):
    """Get single alert detail by ID."""
    alerts = scan_hbase_alerts(limit=500)
    for a in alerts:
        if a.get("alert_id") == alert_id or a.get("id") == alert_id:
            return a

    raise HTTPException(status_code=404, detail=f"Alert '{alert_id}' not found.")


@app.patch("/api/alerts/{alert_id}/status")
def update_alert_status(
    alert_id: str = Path(..., description="Alert identifier"),
    payload: StatusUpdate = None,
):
    """Update status of an alert (open, investigating, resolved) in HBase or simulated store."""
    updated = False

    if HAS_HBASE:
        try:
            conn = get_hbase_connection()
            table_name = os.getenv("HBASE_TABLE", "threatnexus:alerts")
            table = conn.table(table_name)
            matched_key = None
            for key, row in table.scan():
                row_key_str = key.decode(errors="replace")
                extracted_id = row_key_str.split("_", 1)[-1] if "_" in row_key_str else row_key_str
                if extracted_id == alert_id or row_key_str == alert_id:
                    matched_key = key
                    break

            if matched_key:
                table.put(matched_key, {b"details:status": payload.status.encode()})
                updated = True
            conn.close()
        except Exception:
            pass

    for alert in simulated_alerts.values():
        if alert.get("alert_id") == alert_id or alert.get("id") == alert_id:
            alert["status"] = payload.status
            updated = True
            break

    if not updated:
        raise HTTPException(status_code=404, detail=f"Alert '{alert_id}' not found.")

    return {"alert_id": alert_id, "updated_status": payload.status}


@app.get("/api/top-ips")
def get_top_ips(limit: int = Query(default=10, ge=1, le=100)):
    """Retrieve top source IPs with event frequencies and threat metrics."""
    try:
        alerts = scan_hbase_alerts(limit=500)
    except Exception:
        alerts = []

    ip_stats = {}
    for a in alerts:
        ip = a.get("source_ip", "unknown")
        if ip not in ip_stats:
            ip_stats[ip] = {
                "source_ip": ip,
                "total_alerts": 0,
                "highest_severity": "INFO",
                "last_seen": a.get("created_at"),
                "rules_triggered": set(),
            }
        ip_stats[ip]["total_alerts"] += 1
        ip_stats[ip]["rules_triggered"].add(a.get("rule_id") or a.get("type", ""))

        sev = a.get("severity", "INFO").upper()
        if sev == "CRITICAL" or ip_stats[ip]["highest_severity"] != "CRITICAL":
            if sev in ("HIGH", "CRITICAL") or ip_stats[ip]["highest_severity"] not in ("HIGH", "CRITICAL"):
                ip_stats[ip]["highest_severity"] = sev

    # Include recent events buffer count
    for e in recent_events_buffer:
        ip = e.get("source_ip", "unknown")
        if ip not in ip_stats:
            ip_stats[ip] = {
                "source_ip": ip,
                "total_alerts": 0,
                "highest_severity": "INFO",
                "last_seen": e.get("timestamp"),
                "rules_triggered": set(),
            }

    result = []
    for ip, data in ip_stats.items():
        data["rules_triggered"] = list(data["rules_triggered"])
        sev_mult = {"CRITICAL": 50, "HIGH": 30, "MEDIUM": 15, "LOW": 5, "INFO": 1}
        score = min(100, data["total_alerts"] * 15 + sev_mult.get(data["highest_severity"], 1))
        data["threat_score"] = score
        result.append(data)

    result.sort(key=lambda x: (x["threat_score"], x["total_alerts"]), reverse=True)
    return {"top_ips": result[:limit]}


@app.get("/api/events/recent")
def get_recent_events(limit: int = Query(default=50, ge=1, le=200)):
    """Return the most recently ingested security events stream."""
    events_list = list(recent_events_buffer)
    events_list.reverse()
    return {
        "count": len(events_list[:limit]),
        "total_buffered": len(events_list),
        "events": events_list[:limit],
    }


@app.post("/api/events", status_code=202)
def ingest_event(event: Event):
    """Publish a single normalized security event into Kafka and evaluate threat rules."""
    global ingest_counter, last_ingest_time
    topic = os.getenv("KAFKA_TOPIC", "security-events")
    event_dict = event.model_dump()

    published = False
    if HAS_KAFKA:
        try:
            prod = get_kafka_producer()
            prod.send(topic, event_dict).get(timeout=2)
            published = True
        except Exception:
            published = False

    recent_events_buffer.append(event_dict)
    ingest_counter += 1
    last_ingest_time = datetime.now(timezone.utc).isoformat()
    evaluate_streaming_rules(event_dict)

    return {
        "accepted": True,
        "event_id": event.event_id,
        "topic": topic,
        "timestamp": event.timestamp,
        "mode": "kafka" if published else "simulated",
    }


@app.post("/api/events/batch", status_code=202)
def ingest_batch_events(events: List[Event]):
    """Publish a batch of normalized security events into Kafka and evaluate threat rules."""
    global ingest_counter, last_ingest_time
    topic = os.getenv("KAFKA_TOPIC", "security-events")

    producer_instance = None
    if HAS_KAFKA:
        try:
            producer_instance = get_kafka_producer()
        except Exception:
            producer_instance = None

    published = 0
    for event in events:
        event_dict = event.model_dump()
        if producer_instance:
            try:
                producer_instance.send(topic, event_dict)
            except Exception:
                pass
        recent_events_buffer.append(event_dict)
        evaluate_streaming_rules(event_dict)
        published += 1

    if producer_instance:
        try:
            producer_instance.flush()
        except Exception:
            pass

    ingest_counter += published
    last_ingest_time = datetime.now(timezone.utc).isoformat()
    return {"accepted": True, "count": published, "topic": topic}


@app.post("/api/replay/trigger")
def trigger_replay_scenario(scenario: str = Query(..., pattern="^(brute_force|port_scan|process|traffic_spike|normal|cic_ids2017)$")):
    """Trigger a synthetic or CIC-IDS2017 attack replay scenario directly from backend."""
    scripts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
    if scripts_dir not in sys.path:
        sys.path.append(scripts_dir)
    sys.path.append("/opt/threatnexus/scripts")

    events_to_send = []

    if scenario == "brute_force":
        for i in range(7):
            events_to_send.append(Event(
                source_ip="198.51.100.25",
                destination_ip="10.0.0.10",
                destination_port=22,
                event_type="FAILED_LOGIN",
                username="admin" if i % 2 == 0 else "root",
                status="failure",
                severity="HIGH",
                dataset_label="synthetic:brute_force",
                message=f"Brute force SSH trial {i + 1}/7",
            ))
    elif scenario == "port_scan":
        scan_ports = [21, 22, 23, 25, 53, 80, 110, 143, 443, 3306, 8080, 8443]
        for port in scan_ports:
            events_to_send.append(Event(
                source_ip="198.51.100.42",
                destination_ip="10.0.0.10",
                destination_port=port,
                event_type="NETWORK_CONNECTION",
                status="attempted",
                severity="INFO",
                dataset_label="synthetic:port_scan",
                message=f"TCP port probe to {port}",
            ))
    elif scenario == "process":
        events_to_send.append(Event(
            source_ip="198.51.100.99",
            destination_ip="10.0.0.10",
            destination_port=445,
            event_type="PROCESS_EXECUTION",
            process_name="mimikatz",
            status="executed",
            severity="CRITICAL",
            dataset_label="synthetic:process_anomaly",
            message="Unauthorized credential dumper mimikatz spawned",
        ))
    elif scenario == "traffic_spike":
        for i in range(25):
            events_to_send.append(Event(
                source_ip="198.51.100.77",
                destination_ip="10.0.0.20",
                destination_port=80,
                event_type="NETWORK_REQUEST",
                status="completed",
                severity="INFO",
                dataset_label="synthetic:traffic_spike",
                message=f"Burst HTTP request #{i + 1}",
            ))
    elif scenario == "cic_ids2017":
        for i in range(5):
            events_to_send.append(Event(
                source_ip="172.16.0.105",
                destination_ip="192.168.10.50",
                destination_port=22,
                event_type="FAILED_LOGIN",
                status="failure",
                severity="HIGH",
                dataset_label="cic_ids2017:SSH-Patator",
                message="CIC-IDS2017 SSH-Patator brute force attack flow",
            ))
    else:  # normal
        for i in range(5):
            events_to_send.append(Event(
                source_ip=f"192.0.2.{10 + i}",
                destination_ip="10.0.0.10",
                destination_port=443,
                event_type="AUTHENTICATION",
                status="success",
                username=f"user_{i + 1}",
                severity="INFO",
                dataset_label="synthetic:normal",
                message="Normal employee authentication session",
            ))

    return ingest_batch_events(events_to_send)

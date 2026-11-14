# ThreatNexus REST API Specification

This document details the REST endpoints provided by the ThreatNexus FastAPI backend for event ingestion, alert queries, metrics aggregation, and cluster health monitoring.

Base URL: `http://localhost:8000` (or `http://<MASTER_IP>:8000`)  
Interactive Documentation: `http://localhost:8000/docs` (Swagger UI)

---

## Endpoints Summary

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` / `/api/health` | Service health and deep dependency checks (Kafka, HBase, HDFS, Spark) |
| `GET` | `/api/metrics/summary` | Aggregated metrics, total event counts, and alert breakdowns by severity and status |
| `GET` | `/api/alerts` | Paginated, filterable list of security alerts (by severity, status, rule, IP) |
| `GET` | `/api/alerts/{alert_id}` | Detailed inspection of a specific alert by ID |
| `PATCH`| `/api/alerts/{alert_id}/status` | Update alert triage lifecycle status (`open`, `investigating`, `resolved`) |
| `GET` | `/api/top-ips` | Ranked threat intelligence table of most suspicious source IP addresses |
| `GET` | `/api/events/recent` | Real-time sliding window of recent ingested security events |
| `POST`| `/api/events` | Ingest single normalized security event into Kafka `security-events` |
| `POST`| `/api/events/batch` | Ingest multiple security events in a single batch |
| `POST`| `/api/replay/trigger` | Trigger pre-scripted attack scenario replay (`brute_force`, `port_scan`, `process`, `cic_ids2017`) |

---

## Detailed Specifications

### 1. `GET /api/health`
Returns system operational status and live socket probes for backend dependencies.

**Response Example (200 OK):**
```json
{
  "status": "ok",
  "service": "threatnexus-api",
  "timestamp": "2026-11-10T12:00:00Z",
  "overall_status": "healthy",
  "dependencies": {
    "kafka": { "status": "healthy", "broker": "kafka:29092" },
    "hbase": { "status": "healthy", "host": "hbase", "tables": ["threatnexus:alerts", "threatnexus:ip_summary", "threatnexus:system_metrics"] },
    "hdfs": { "status": "healthy", "namenode": "namenode:9000" },
    "spark": { "status": "healthy", "master": "spark://spark-master:7077" }
  }
}
```

---

### 2. `GET /api/metrics/summary`
Returns high-level platform KPIs for the dashboard overview.

**Response Example (200 OK):**
```json
{
  "total_events_ingested": 145,
  "recent_events_buffered": 82,
  "total_alerts": 8,
  "alerts_by_severity": {
    "CRITICAL": 1,
    "HIGH": 5,
    "MEDIUM": 2,
    "LOW": 0,
    "INFO": 0
  },
  "alerts_by_status": {
    "open": 6,
    "investigating": 1,
    "resolved": 1
  },
  "top_triggered_rules": {
    "BRUTE_FORCE_001": 4,
    "PORT_SCAN_004": 2,
    "SUSPICIOUS_PROC_005": 1,
    "CIC_DATASET_ATTACK_006": 1
  },
  "system_status": "ELEVATED_THREAT"
}
```

---

### 3. `GET /api/alerts`
Query parameters:
- `limit` (int, default=50, max=500)
- `severity` (string, optional: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`)
- `status` (string, optional: `open`, `investigating`, `resolved`)
- `rule_id` (string, optional)
- `ip` (string, optional)

**Response Example (200 OK):**
```json
{
  "count": 1,
  "total_scanned": 8,
  "alerts": [
    {
      "id": "alert-bruteforce-7c9e1a4f02b8",
      "alert_id": "alert-bruteforce-7c9e1a4f02b8",
      "rule_id": "BRUTE_FORCE_001",
      "severity": "HIGH",
      "source_ip": "198.51.100.25",
      "event_count": "7",
      "description": "Repeated failed logins detected: 7 attempts within 60s",
      "status": "open",
      "created_at": "2026-11-10 12:00:25"
    }
  ]
}
```

---

### 4. `PATCH /api/alerts/{alert_id}/status`
Update triage lifecycle stage in HBase.

**Request Body:**
```json
{
  "status": "investigating"
}
```

**Response Example (200 OK):**
```json
{
  "alert_id": "alert-bruteforce-7c9e1a4f02b8",
  "updated_status": "investigating"
}
```

---

### 5. `GET /api/top-ips`
Returns ranked list of suspect IP addresses sorted by computed threat score.

**Response Example (200 OK):**
```json
{
  "top_ips": [
    {
      "source_ip": "198.51.100.25",
      "total_alerts": 4,
      "highest_severity": "HIGH",
      "threat_score": 90,
      "rules_triggered": ["BRUTE_FORCE_001"],
      "last_seen": "2026-11-10 12:00:25"
    }
  ]
}
```

---

### 6. `POST /api/events`
Ingest single event into Kafka topic `security-events`.

**Request Body:**
```json
{
  "source_ip": "198.51.100.25",
  "destination_ip": "10.0.0.10",
  "destination_port": 22,
  "event_type": "FAILED_LOGIN",
  "username": "root",
  "status": "failure",
  "severity": "HIGH",
  "message": "SSH login failure"
}
```

**Response Example (202 Accepted):**
```json
{
  "accepted": true,
  "event_id": "evt-c48f21901a2b",
  "topic": "security-events",
  "timestamp": "2026-11-10T12:00:00Z"
}
```


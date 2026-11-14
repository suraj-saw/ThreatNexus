# ThreatNexus Architecture Reference
**System Title:** Distributed Real-Time Cybersecurity Threat Detection & Log Analytics Platform

---

## 1. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph Data_Sources ["1. Data Sources"]
        S1["Synthetic Event Generator<br/>(Normal, Bursts, Scans, Attacks)"]
        S2["CIC-IDS2017 Dataset Replay<br/>(SSH-Patator, PortScan, DoS)"]
        S3["Custom REST API Ingest<br/>(JSON Payloads)"]
    end

    subgraph Ingestion ["2. Ingestion & Buffering Layer"]
        K1["Apache Kafka Broker<br/>Topic: security-events<br/>Port: 9092 / 29092"]
        ZK["Apache ZooKeeper<br/>Port: 2181"]
        ZK --- K1
    end

    subgraph Processing ["3. Stream Processing & Threat Detection Engine"]
        SP["Apache Spark Structured Streaming<br/>(PySpark / Scala Connector)<br/>Watermark: 2 minutes"]
        
        R1["Rule 1: BRUTE_FORCE_001<br/>(>=5 Failed Logins / 60s)"]
        R2["Rule 2: PORT_SCAN_004<br/>(>=10 Destination Ports / 60s)"]
        R3["Rule 3: TRAFFIC_SPIKE_003<br/>(>=20 Events / 60s per IP)"]
        R4["Rule 4: SUSPICIOUS_PROC_005<br/>(Denylist: mimikatz, nmap, nc)"]
        R5["Rule 5: CIC_DATASET_ATTACK_006<br/>(Labelled Flow Attack Category)"]
        
        SP --> R1
        SP --> R2
        SP --> R3
        SP --> R4
        SP --> R5
    end

    subgraph Storage ["4. Distributed Big Data Storage"]
        HDFS["Hadoop HDFS (NameNode & DataNodes)<br/>Raw & Curated Event History<br/>/threatnexus/events/date=YYYY-MM-DD<br/>Port: 9000 / 9870"]
        HBASE["Apache HBase (Master & RegionServers)<br/>- threatnexus:alerts (Details)<br/>- threatnexus:ip_summary (Stats)<br/>- threatnexus:system_metrics (Value)<br/>Reverse Timestamp Row Keys<br/>Port: 16000 / 9090 Thrift"]
    end

    subgraph Serving ["5. Application & Presentation Layer"]
        API["FastAPI REST Backend<br/>- Ingestion & Validation<br/>- Multi-Node Health Probes<br/>- Alerts, Metrics, Top-IPs<br/>Port: 8000"]
        UI["React SOC Web Dashboard<br/>- Executive Overview & KPIs<br/>- Live Streaming Event Log<br/>- Filterable Alerts Center<br/>- IP Intelligence Profiler<br/>- Cluster Health Topology<br/>- One-Click Attack Simulator<br/>Port: 3000"]
    end

    S1 -->|JSON over Kafka/HTTP| K1
    S2 -->|Normalized JSON| K1
    S3 -->|Validated Ingestion| API
    API -->|Produce UTF-8 JSON| K1
    
    K1 -->|Subscribe security-events| SP
    
    SP -->|Date-Partitioned Append| HDFS
    SP -->|Batch Put via Thrift| HBASE
    
    HBASE -->|Scan & Retrieve Lookups| API
    API -->|JSON REST Responses| UI
    UI -->|Trigger Replay / Custom Events| API
```

---

## 2. Component Responsibilities & Verification

| Technology | Responsibility | What to Demonstrate in Demo |
| :--- | :--- | :--- |
| **Apache Kafka** | Buffers and distributes high-throughput incoming security events with zero loss. | Producer publishes UTF-8 JSON events; Spark consumer reads topic partition offsets. |
| **Apache Spark Structured Streaming** | Parses JSON, manages event-time windows and 2-min watermarks, evaluates detection rules, and outputs alerts. | Streaming query metrics, micro-batch processing logs, and real-time alert trigger emission. |
| **Hadoop HDFS** | Distributed fault-tolerant storage for historical raw and curated event logs. | HDFS NameNode Web UI (`:9870`), date-partitioned event directories (`/threatnexus/events/date=...`), live DataNode block reports. |
| **Apache HBase** | Distributed NoSQL column store for rapid random lookups of alerts, IP intelligence, and system metrics. | Reverse-timestamp row keys for fast latest-first scans; HBase shell table scans (`scan 'threatnexus:alerts'`). |
| **FastAPI Backend** | High-performance async REST API providing data ingestion, validation, health monitoring, and analytics endpoints. | Swagger UI (`/docs`), `/api/health` multi-node probe, `/api/alerts`, `/api/top-ips`. |
| **React Dashboard** | Professional SOC dark-mode web application providing real-time visibility into security events and alerts. | Real-time KPI cards, live event stream, interactive alerts triage, IP threat scoring, cluster topology status. |

---

## 3. Storage Schemas

### Hadoop HDFS Storage Layout
- Raw & Curated Event Logs:
  `hdfs://namenode:9000/threatnexus/events/date=YYYY-MM-DD/part-*.json`
- Streaming Checkpoints:
  `hdfs://namenode:9000/threatnexus/checkpoints/alerts_v2/`
  `hdfs://namenode:9000/threatnexus/checkpoints/events_v2/`

### Apache HBase Table Schema
1. **`threatnexus:alerts`**
   - **Row Key:** `{rev_ts}_{alert_id}` (where `rev_ts = 9999999999999 - unix_ms`)
   - **Column Family `details:`**
     `alert_id`, `rule_id`, `severity`, `type`, `source_ip`, `description`, `event_count`, `window_start`, `window_end`, `status`, `created_at`
2. **`threatnexus:ip_summary`**
   - **Row Key:** `{source_ip}_{YYYY-MM-DD}`
   - **Column Family `stats:`**
     `last_seen`, `last_alert`, `severity`, `event_count`, `failed_logins`
3. **`threatnexus:system_metrics`**
   - **Row Key:** `{metric_name}_{time_bucket}`
   - **Column Family `value:`**
     `count`, `rate`, `timestamp`

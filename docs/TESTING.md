# ThreatNexus Verification, Testing & Troubleshooting Guide

This document outlines the test cases, verification procedures, and troubleshooting guidelines for the ThreatNexus distributed pipeline.

---

## 1. Test Verification Matrix

| Test Case | Scenario / Condition | Execution | Expected Result | Verified Status |
| :--- | :--- | :--- | :--- | :--- |
| **T1: Valid Event Ingestion** | Ingest schema-compliant JSON event | `POST /api/events` with valid event | HTTP 202 Accepted, published to Kafka `security-events`, parsed by Spark, archived to HDFS. | **PASS** |
| **T2: Malformed JSON Handling** | Ingest corrupted / invalid schema event | `POST /api/events` with missing fields | HTTP 422 Unprocessable Entity; stream processor drops or routes to bad-record path; streaming engine does not crash. | **PASS** |
| **T3: Failed-Login Brute Force** | 7 failed logins from single IP within 30s | `python scripts/replay_dataset.py --scenario brute_force` | Spark triggers `BRUTE_FORCE_001` alert (`HIGH` severity); written to HBase `threatnexus:alerts` with reverse timestamp row key. | **PASS** |
| **T4: Normal Traffic Baseline** | Benign employee authentication sessions | `python scripts/replay_dataset.py --scenario normal` | Ingested and stored in HDFS; zero brute-force alerts emitted. | **PASS** |
| **T5: Port Scan Sweep** | 12 distinct TCP port connections in 30s | `python scripts/replay_dataset.py --scenario port_scan` | Spark triggers `PORT_SCAN_004` alert (`HIGH` severity, Horizontal sweep). | **PASS** |
| **T6: Suspicious Process Execution** | Denylisted process name (`mimikatz`) | `python scripts/replay_dataset.py --scenario process` | Spark triggers `SUSPICIOUS_PROC_005` alert (`CRITICAL` severity). | **PASS** |
| **T7: CIC-IDS2017 Dataset Replay** | Replay labelled flow attack records | `python scripts/replay_dataset.py --source csv --input data/cic_ids2017_sample.csv` | Spark detects `CIC_DATASET_ATTACK_006` matching labelled category (`SSH-Patator`, `PortScan`). | **PASS** |
| **T8: Late Event Handling** | Event with timestamp older than 2 minutes | Ingest event with timestamp older than watermark | Watermark policy ("2 minutes") discards from tumbling window without corrupting state store. | **PASS** |
| **T9: Spark Restart & Recovery** | Restart `spark-streaming` container | `docker compose restart spark-streaming` | Query resumes processing from HDFS checkpoint without re-emitting duplicate historical alerts. | **PASS** |
| **T10: Multi-Component Health Check** | Deep probe of Kafka, HBase, HDFS, Spark | `GET /api/health` | Returns component-level status, connection hostnames, and tables. | **PASS** |

---

## 2. Automated Test Suite Execution

ThreatNexus includes an automated end-to-end testing script that validates the full API and detection pipeline:

```bash
python scripts/test_pipeline.py --api http://localhost:8000
```

---

## 3. Common Troubleshooting Scenarios & Solutions

### Issue 1: Spark Streaming Fails to Connect to Kafka
- **Symptom:** Spark logs show `NoBrokersAvailable` or `TimeoutException`.
- **Cause:** Kafka advertised listeners configured to `localhost` inside container network.
- **Solution:** In Docker Compose, Kafka advertised listeners must use the Docker bridge name `PLAINTEXT://kafka:29092`. For remote worker PCs, set `MASTER_IP` in `.env` to the LAN IP.

### Issue 2: HBase Thrift Connection Refused (`port 9090`)
- **Symptom:** API returns `503 Service Unavailable: HBase not ready`.
- **Cause:** HBase Master and RegionServer take 15-30 seconds to initialize ZooKeeper znode locks before Thrift starts.
- **Solution:** Check HBase status: `docker compose logs --tail=50 hbase-master hbase`. ThreatNexus Compose includes health dependencies and retries.

### Issue 3: HDFS Safe Mode Blocks Checkpoints
- **Symptom:** Spark cannot create checkpoint directory `hdfs://namenode:9000/threatnexus/checkpoints`.
- **Cause:** NameNode started with safe mode ON while waiting for DataNode block reports.
- **Solution:** Leave safe mode manually:
  ```bash
  docker exec -it namenode hdfs dfsadmin -safemode leave
  ```

### Issue 4: Duplicate Alerts on Stream Restart
- **Symptom:** Repeated alerts appearing in HBase after restarting containers.
- **Cause:** Checkpoints cleared or non-deterministic alert IDs.
- **Solution:** ThreatNexus generates deterministic alert IDs using SHA-256 hashes of `source_ip` and `window.start`, ensuring idempotent upserts in HBase.

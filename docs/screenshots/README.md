# ThreatNexus — System Demonstration Screenshots

This directory contains visual captures of the running ThreatNexus distributed cluster, web user interfaces, and automated test suite verification.

---

### 1. React SOC Dashboard Overview
Executive security monitoring dashboard displaying active threat level, KPI summaries, live threat velocity sparklines, and alignment with the MITRE ATT&CK framework.
![React SOC Dashboard](React_SOC_Dashboard.jpeg)

---

### 2. Live Security Event Stream
High-throughput event log feed streaming directly from the Apache Kafka `security-events` topic into the React dashboard via WebSocket/REST polling.
![Live Security Events](Live_Security_Events.jpeg)

---

### 3. Alerts Triage Center
Real-time incident management interface showing detected threats (`BRUTE_FORCE_001`, `PORT_SCAN_004`, `SUSPICIOUS_PROC_005`), severity tags (`CRITICAL`, `HIGH`, `MEDIUM`), and interactive status transitions (`open` → `investigating` → `resolved`).
![Alerts Triage Center](Alert_Triangle_Center.jpeg)

---

### 4. One-Click Attack Simulator
Interactive attack replay console allowing one-click execution of synthetic attack sequences (SSH Brute Force, TCP Port Scan, Suspicious Process Execution, and Volumetric Traffic Surges) for live demonstration.
![One-Click Attack Simulator](One_Click_Attack_Simulator.jpeg)

---

### 5. Hadoop HDFS NameNode Web UI (`http://localhost:9870`)
Demonstrates distributed file system health and cluster storage capacity, confirming **`Live datanodes (2)`** (`worker1-datanode` and `worker2-datanode`) with active block replication.
![HDFS NameNode Web UI](HDFS_Namenode_Web_UI.jpeg)

---

### 6. Apache Spark Master Web UI (`http://localhost:8080`)
Standalone cluster manager interface confirming **2 active worker nodes** (`worker1-spark` and `worker2-spark`) with allocated compute cores and the running `ThreatNexus-Detection-Engine` structured streaming application.
![Spark Master Web UI](Spark_Master_Web_UI.jpeg)

---

### 7. Apache Spark Worker 1 Web UI (`http://localhost:8081`)
Direct executor dashboard for Worker 1 (`worker1-spark`) confirming 2 allocated cores, 1.0 GiB memory, and active streaming task execution for the `ThreatNexus-Detection-Engine` application.
![Spark Worker 1 Web UI](Spark_Worker_1_web_UI.jpeg)

---

### 8. Apache Spark Worker 2 Web UI (`http://localhost:8082`)
Direct executor dashboard for Worker 2 (`worker2-spark`) confirming 2 allocated cores, 1.0 GiB memory, and concurrent micro-batch task processing alongside Worker 1.
![Spark Worker 2 Web UI](Spark_Worker_2_Web_UI.jpeg)

---

### 9. Apache HBase Master Web UI (`http://localhost:16010`)
Distributed NoSQL database interface showing **2 live RegionServers**, active region balance, and the initialized schema tables (`threatnexus:alerts`, `threatnexus:ip_summary`, `threatnexus:system_metrics`).
![HBase Master Web UI](HBASE_Master_Web_UI.jpeg)

---

### 10. Automated Pipeline Verification Suite
Terminal output from running `scripts/test_pipeline.py`, demonstrating that all **11 out of 11 automated test cases passed (100% pass rate)** across API health, Kafka ingestion, detection heuristics, and big data persistence.
![Automated Verification Suite](Automated_Verification_Suite.jpeg)

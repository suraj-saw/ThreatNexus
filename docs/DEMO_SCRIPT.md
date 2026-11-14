# ThreatNexus 8-Minute Evaluation Demonstration Script
Derived from Section 14 & 15 of the Scalable Implementation Guide.

This structured walkthrough provides exact talking points, actions, and terminal commands to demonstrate the platform to evaluators within an 8-minute presentation window.

---

## Timeline & Demonstration Flow

| Time Window | Segment | Goal / What to Show | Action & Command |
| :--- | :--- | :--- | :--- |
| **0:00 – 1:00** | **Problem, Architecture & Topology** | Explain problem statement: streaming log analytics for cyber threat detection. Show architecture diagram and cluster layout. | Show `docs/ARCHITECTURE.md` diagram. Highlight Kafka buffering, Spark windowed detection, HDFS raw event archiving, and HBase alert persistence. |
| **1:00 – 2:00** | **Master & Worker Nodes Registration** | Prove distributed multi-node architecture (1 Master + 2 Workers). | 1. Open HDFS UI: `http://localhost:9870` or run `docker exec -it namenode hdfs dfsadmin -report` to show live DataNodes.<br/>2. Open Spark UI: `http://localhost:8080` to show registered workers and cores.<br/>3. Open HBase UI: `http://localhost:16010` to show registered RegionServers. |
| **2:00 – 3:00** | **Kafka Ingestion & Stream Replay** | Demonstrate real-time security log ingestion into Kafka topic `security-events`. | Run the replay tool:<br/>`python scripts/replay_dataset.py --scenario normal --rate 5 --limit 15`<br/>Show live events appearing on the React dashboard Live Events tab (`http://localhost:3000`). |
| **3:00 – 4:30** | **Spark Processing & Triggered Alert** | Trigger the synthetic brute-force scenario and show Spark Structured Streaming detecting it. | 1. Execute attack replay:<br/>`python scripts/replay_dataset.py --scenario brute_force`<br/>2. Show Spark logs: `docker compose logs --tail=20 spark-streaming`<br/>3. Show immediate alert emission in React Alerts Center (`BRUTE_FORCE_001` with 7 attempts in 30s). |
| **4:30 – 5:30** | **HDFS Files & HBase Alert Records** | Prove persistence in big data storage layer. | 1. HDFS check: `docker exec -it namenode hdfs dfs -ls -R /threatnexus/events`<br/>2. Show date-partitioned JSON files.<br/>3. HBase check: `docker exec -it hbase hbase shell`<br/>Run: `scan 'threatnexus:alerts', {LIMIT => 5}`<br/>Show reverse-timestamp row keys. |
| **5:30 – 7:00** | **React SOC Dashboard & Analytics** | Showcase the 6 dashboard screens, alert triage actions, IP intelligence, and system health. | 1. **Overview Tab**: Show KPI counts, elevated threat status.<br/>2. **Alerts Center Tab**: Demonstrate filtering by severity and status; update an alert from `open` to `investigating`.<br/>3. **IP Intelligence Tab**: Show suspect IP ranking, threat score (90/100).<br/>4. **Cluster Health Tab**: Show live status dots for Kafka, Spark, HDFS, HBase, API. |
| **7:00 – 8:00** | **Worker Expansion & Future Scope** | Explain how to scale from 2 to 5+ workers using `docs/cluster-inventory.md` with zero code changes. | Highlight worker-agnostic design, HDFS balancer, dynamic Spark worker registration, and recap milestone checklist completion. |

---

## Quick Reference Commands

### Start Services (Single Machine / Dev Mode)
```bash
docker compose up -d --build
```

### Run Automated End-to-End Test Suite
```bash
python scripts/test_pipeline.py
```

### Ingest Pre-Scripted Scenarios
```bash
# 1. SSH Brute Force
python scripts/replay_dataset.py --scenario brute_force

# 2. Port Scan Sweep
python scripts/replay_dataset.py --scenario port_scan

# 3. Suspicious Process (mimikatz)
python scripts/replay_dataset.py --scenario process

# 4. CIC-IDS2017 Dataset Flow Replay
python scripts/replay_dataset.py --source csv --input data/cic_ids2017_sample.csv
```


# ThreatNexus Cluster Inventory & Worker Scaling Guide

This document details the node allocation, network topology, and onboarding procedure for expanding the ThreatNexus cluster beyond 2 workers.

## 1. Node Inventory

| Node Name | Role | Physical LAN IP | Single-PC Simulation Hostname & Ports | Services Running | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **master** | Master Node | `192.168.1.10` | `threatnexus-master` (Default ports) | NameNode, Spark Master, Kafka Broker, HBase Master, HBase Thrift, FastAPI, React UI | Active |
| **worker1** | Worker Node 1 | `192.168.1.11` | `worker1` (Spark UI: `8081`, HDFS: `9864`, HBase: `16020`) | DataNode 1, Spark Worker 1, HBase RegionServer 1 | Active |
| **worker2** | Worker Node 2 | `192.168.1.12` | `worker2` (Spark UI: `8082`, HDFS: `9865`, HBase: `16021`) | DataNode 2, Spark Worker 2, HBase RegionServer 2 | Active |
| **worker3** | Expansion Node 3 | `192.168.1.13` | `worker3` (Spark UI: `8083`, HDFS: `9868`, HBase: `16022`) | DataNode 3, Spark Worker 3, HBase RegionServer 3 | Standby / Planned |
| **worker4** | Expansion Node 4 | `192.168.1.14` | `worker4` (Spark UI: `8084`, HDFS: `9871`, HBase: `16023`) | DataNode 4, Spark Worker 4, HBase RegionServer 4 | Standby / Planned |
| **worker5** | Expansion Node 5 | `192.168.1.15` | `worker5` (Spark UI: `8085`, HDFS: `9872`, HBase: `16024`) | DataNode 5, Spark Worker 5, HBase RegionServer 5 | Standby / Planned |

> **Single-PC Simulation Note:** In local simulation mode on a single machine, all nodes connect over the Docker bridge network `threatnexus-net`. Adding expansion workers locally only requires running another `docker compose -p threatnexus-worker<N>` instance with incremented port environment variables (`SPARK_WORKER_UI_PORT`, `DATANODE_HTTP_PORT`, `DATANODE_DATA_PORT`, `REGIONSERVER_PORT`, `REGIONSERVER_INFO_PORT`).

---

## 2. Repeatable Worker Onboarding Procedure

Adding a worker node to ThreatNexus requires **zero changes** to application code (producers, Spark streaming detection rules, backend APIs, and React UI are 100% worker-agnostic).

### Step 1: Prepare the New Physical Machine / VM
1. Install Docker Engine and Docker Compose v2.
2. Assign a static IP on the trusted local network (e.g. `192.168.1.13` for `worker3`).
3. Ensure time synchronization via NTP:
   ```bash
   sudo timedatectl set-ntp on
   ```
4. Verify network reachability to Master PC (`192.168.1.10`):
   ```bash
   ping 192.168.1.10
   nc -zv 192.168.1.10 9000  # Test HDFS RPC
   nc -zv 192.168.1.10 7077  # Test Spark Master
   nc -zv 192.168.1.10 2181  # Test ZooKeeper
   ```

### Step 2: Configure Environment on New Worker
1. Copy the repository or minimum worker bundle (`docker-compose.worker.yml`, `config/`, and `spark/Dockerfile`) to the worker machine.
2. Create `.env` on the worker:
   ```bash
   MASTER_IP=192.168.1.10
   WORKER_HOSTNAME=worker3
   SPARK_WORKER_CORES=2
   SPARK_WORKER_MEMORY=2G
   ```

### Step 3: Start Worker Services
```bash
docker compose -f docker-compose.worker.yml up -d
```

### Step 4: Verify Cluster Registration
1. **HDFS DataNode Registration**:
   On the Master PC:
   ```bash
   docker exec -it namenode hdfs dfsadmin -report
   ```
   Verify that `Live datanodes` count increments and `worker3` appears with healthy capacity.

2. **Spark Worker Registration**:
   Open the Spark Web UI: `http://192.168.1.10:8080`.
   Verify the new worker appears under **Alive Workers** with the configured memory and cores.

3. **HBase RegionServer Registration**:
   Open the HBase Master Web UI: `http://192.168.1.10:16010`.
   Verify that the new RegionServer appears in the **Region Servers** table.

### Step 5: Rebalance Storage & Regions (Optional for Active Clusters)
- **HDFS Balancer**:
  If significant data already exists in HDFS, trigger block rebalancing:
  ```bash
  docker exec -it namenode hdfs balancer -threshold 10
  ```
- **HBase Load Balancer**:
  HBase automatically balances regions across available RegionServers via its periodic balancer thread. To trigger manually:
  ```bash
  docker exec -it hbase-master hbase shell
  > balance_switch true
  > balancer
  ```

---

## 3. Scaling Impact Matrix

| Component | What changes when workers are added? | ThreatNexus Code Change? |
| :--- | :--- | :--- |
| **HDFS** | New DataNodes register with NameNode. Receives new block writes. Existing blocks rebalance with `hdfs balancer`. | **No** |
| **Spark Standalone** | New Spark Worker registers with Spark Master. Spark automatically schedules future streaming micro-batch tasks across more cores. | **No** |
| **HBase** | New RegionServer registers with HBase Master. HBase balancer redistributes region splits. | **No** |
| **Kafka** | No change required if keeping single or dual broker setup. | **No** |
| **FastAPI Backend** | No change required. Communicates with Master Thrift & Kafka. | **No** |
| **React Dashboard** | No change required. Connects to stable FastAPI endpoint. | **No** |


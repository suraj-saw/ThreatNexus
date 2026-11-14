# ThreatNexus Distributed Multi-Node Deployment Guide
**Architecture Target:** 1 Master Node + at least 2 Worker Nodes (Simulated locally on a single PC OR deployed across physical LAN PCs).

---

## 1. Overview & Service Placement

In adherence with the course specifications, ThreatNexus is architected for distributed cluster deployment across physical machines connected over a trusted local area network (LAN), as well as **local multi-terminal cluster simulation on a single PC** using Docker Compose project namespaces and isolated port offsets over a shared bridge network (`threatnexus-net`).

```
                      +---------------------------------------+
                      |               MASTER NODE             |
                      |        (LAN IP or localhost)          |
                      |                                       |
                      |  * HDFS NameNode      (RPC: 9000)     |
                      |  * Spark Master       (RPC: 7077)     |
                      |  * Kafka Broker       (Port: 9092)    |
                      |  * ZooKeeper Quorum   (Port: 2181)    |
                      |  * HBase Master       (Port: 16000)   |
                      |  * HBase Thrift API   (Port: 9090)    |
                      |  * ThreatNexus API    (Port: 8000)    |
                      |  * ThreatNexus UI     (Port: 3000)    |
                      +-------------------+-------------------+
                                          |
                        Private Network / Docker Bridge (threatnexus-net)
                                          |
            +-----------------------------+-----------------------------+
            |                                                           |
+-----------v---------------------------+   +---------------------------v-----------+
|              WORKER 1                 |   |              WORKER 2                 |
|    (Physical PC 1 OR Terminal 2)      |   |    (Physical PC 2 OR Terminal 3)      |
|                                       |   |                                       |
|  * HDFS DataNode 1   (9864 / 9866)    |   |  * HDFS DataNode 2   (9865 / 9867)*   |
|  * Spark Worker 1    (UI: 8081)       |   |  * Spark Worker 2    (UI: 8082)*      |
|  * HBase RegionServer 1 (16020/16030) |   |  * HBase RegionServer 2 (16021/16031)*|
+---------------------------------------+   +---------------------------------------+
(* Port offsets applied in single-PC simulation mode to avoid host port collisions)
```

---

## 2. Hardware & Environment Requirements

### A. Single-PC Local Cluster Simulation (1 Master + 2 Workers on 1 Machine)
- **Host OS:** Windows 10/11 (64-bit) with WSL2 (Ubuntu), macOS (Apple Silicon/Intel), or Linux (Ubuntu 22.04+).
- **CPU:** Quad-core (8 threads) minimum; 6 to 8 cores recommended.
- **Memory (RAM):**
  - **16 GB RAM minimum** (24 GB or 32 GB recommended).
  - *For Windows WSL2 users:* Ensure `C:\Users\<Username>\.wslconfig` allocates at least **12 GB RAM** and **4+ vCPUs** to the WSL2 VM:
    ```ini
    [wsl2]
    memory=12GB
    processors=6
    swap=4GB
    ```
- **Disk Space:** Minimum 30 GB free storage (preferably NVMe/SSD) for Docker images and HDFS data volumes.
- **Software Prerequisites:**
  - Docker Desktop with Compose v2 (or Docker Engine + Compose plugin on Linux).
  - Python 3.10+ (installed in WSL2 or on host for verification and replay scripts).
  - `curl` utility.

### B. Physical Multi-PC Deployment (Separate Machines over LAN)
- **Master PC:** Quad-core CPU, minimum 16 GB RAM, 50 GB free disk space.
- **Worker 1 & 2 PCs:** Dual-core+ CPU, minimum 8 GB RAM, 30 GB free disk space.
- **LAN Switch:** Gigabit Ethernet or low-latency private Wi-Fi (5 GHz).

---

## 3. Deployment Mode A: Single-PC Multi-Terminal Simulation (Recommended for Evaluation)

This mode runs the authentic distributed architecture (Master + Worker 1 + Worker 2) on a single machine across 4 terminal windows without needing 3 physical laptops.

### Architecture Highlights on Single Machine
1. **Shared Network (`threatnexus-net`):** Master and Workers communicate through a dedicated Docker bridge network with internal DNS resolution (`namenode:9000`, `zookeeper:2181`, `spark-master:7077`, `hbase:9090`).
2. **Project Namespaces:** Master runs under `-p threatnexus-master`, Worker 1 under `-p threatnexus-worker1`, and Worker 2 under `-p threatnexus-worker2`.
3. **Port Offsets:** Worker 2 maps host ports to `9865:9864`, `9867:9866`, `8082:8081`, `16021:16020`, `16031:16030` so both workers run concurrently without port conflicts.
4. **Isolated Hostnames:** Containers receive distinct hostnames (`worker1-datanode`, `worker1-spark`, `worker1-regionserver` vs `worker2-...`) so HDFS, Spark, and HBase track each node independently.

### Step 3.1: Clean Up Old Stacks (Run Once)
In PowerShell or WSL2/bash from the project root:
```bash
# Windows PowerShell
docker compose -p threatnexus-worker1 -f docker-compose.worker.yml down -v --remove-orphans
docker compose -p threatnexus-worker2 -f docker-compose.worker.yml down -v --remove-orphans
docker compose -p threatnexus-master -f docker-compose.master.yml down -v --remove-orphans
```

### Step 3.2: Terminal 1 — Start Master Stack
Ensure `.env` contains:
```env
MASTER_IP=localhost
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=security-events
HBASE_HOST=hbase
HBASE_THRIFT_PORT=9090
HBASE_TABLE=threatnexus:alerts
HDFS_OUTPUT_PATH=hdfs://namenode:9000/threatnexus/events
SPARK_CHECKPOINT_BASE=hdfs://namenode:9000/threatnexus/checkpoints
CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

Start the Master stack:
```powershell
# PowerShell
docker compose -p threatnexus-master -f docker-compose.master.yml up -d --build
docker compose -p threatnexus-master -f docker-compose.master.yml ps
```
*(Or in WSL2 bash: `docker compose -p threatnexus-master -f docker-compose.master.yml up -d --build`)*

### Step 3.3: Terminal 2 — Start Worker 1
```powershell
# Windows PowerShell
$env:WORKER_HOSTNAME = "worker1"
$env:DATANODE_HTTP_PORT = "9864"
$env:DATANODE_DATA_PORT = "9866"
$env:SPARK_WORKER_UI_PORT = "8081"
$env:REGIONSERVER_PORT = "16020"
$env:REGIONSERVER_INFO_PORT = "16030"
docker compose -p threatnexus-worker1 -f docker-compose.worker.yml up -d --build
docker compose -p threatnexus-worker1 -f docker-compose.worker.yml ps
```
*(Or in WSL2 bash: `WORKER_HOSTNAME=worker1 DATANODE_HTTP_PORT=9864 DATANODE_DATA_PORT=9866 SPARK_WORKER_UI_PORT=8081 REGIONSERVER_PORT=16020 REGIONSERVER_INFO_PORT=16030 docker compose -p threatnexus-worker1 -f docker-compose.worker.yml up -d --build`)*

### Step 3.4: Terminal 3 — Start Worker 2
```powershell
# Windows PowerShell
$env:WORKER_HOSTNAME = "worker2"
$env:DATANODE_HTTP_PORT = "9865"
$env:DATANODE_DATA_PORT = "9867"
$env:SPARK_WORKER_UI_PORT = "8082"
$env:REGIONSERVER_PORT = "16021"
$env:REGIONSERVER_INFO_PORT = "16031"
docker compose -p threatnexus-worker2 -f docker-compose.worker.yml up -d --build
docker compose -p threatnexus-worker2 -f docker-compose.worker.yml ps
```
*(Or in WSL2 bash: `WORKER_HOSTNAME=worker2 DATANODE_HTTP_PORT=9865 DATANODE_DATA_PORT=9867 SPARK_WORKER_UI_PORT=8082 REGIONSERVER_PORT=16021 REGIONSERVER_INFO_PORT=16031 docker compose -p threatnexus-worker2 -f docker-compose.worker.yml up -d --build`)*

### Step 3.5: Terminal 4 — Verification & Testing
In WSL2 bash (or PowerShell):
```bash
# 1. Verify HDFS shows 2 Live DataNodes and Safe Mode is OFF
docker compose -p threatnexus-master -f docker-compose.master.yml exec namenode hdfs dfsadmin -report
docker compose -p threatnexus-master -f docker-compose.master.yml exec namenode hdfs dfsadmin -safemode get

# 2. Verify HBase shows 2 active RegionServers and schema tables
docker compose -p threatnexus-master -f docker-compose.master.yml exec hbase-master bash -c "printf 'status\nlist\n' | /usr/local/hbase/bin/hbase shell -n"

# 3. Replay attack scenario (SSH Brute Force)
python3 scripts/replay_dataset.py --scenario brute_force --target http://localhost:8000/api/events

# 4. Run automated end-to-end verification suite (11 test cases)
python3 scripts/test_pipeline.py --api http://localhost:8000

# 5. Verify persisted alerts in HBase and raw partitions in HDFS
curl -s http://localhost:8000/api/alerts | python3 -m json.tool
docker compose -p threatnexus-master -f docker-compose.master.yml exec namenode hdfs dfs -ls -R /threatnexus/events
```

---

## 4. Deployment Mode B: Physical Multi-PC Deployment (LAN Cluster)

When physical lab computers are available:

### Network Setup:
- Assign static IP addresses (or DHCP reservations):
  - Master PC: `192.168.1.10`
  - Worker 1 PC: `192.168.1.11`
  - Worker 2 PC: `192.168.1.12`
- Configure `/etc/hosts` (or Windows `hosts` file):
  ```
  192.168.1.10 master
  192.168.1.11 worker1
  192.168.1.12 worker2
  ```

### Startup Procedure on Physical PCs:
1. **Master PC (`192.168.1.10`):**
   ```bash
   cp config/master.env.example .env
   # Ensure MASTER_IP=192.168.1.10
   docker compose -p threatnexus-master -f docker-compose.master.yml up -d --build
   ```
2. **Worker 1 PC (`192.168.1.11`):**
   ```bash
   cp config/worker.env.example .env
   # Ensure MASTER_IP=192.168.1.10, WORKER_HOSTNAME=worker1
   docker compose -p threatnexus-worker1 -f docker-compose.worker.yml up -d --build
   ```
3. **Worker 2 PC (`192.168.1.12`):**
   ```bash
   cp config/worker.env.example .env
   # Ensure MASTER_IP=192.168.1.10, WORKER_HOSTNAME=worker2
   docker compose -p threatnexus-worker2 -f docker-compose.worker.yml up -d --build
   ```

---

## 5. Web Dashboard & UI Endpoints

| Service | Single-PC Local URL | Physical LAN URL | Credentials / Notes |
| :--- | :--- | :--- | :--- |
| **React SOC Dashboard** | `http://localhost:3000` | `http://<MASTER_IP>:3000` | Real-time threat visualizer |
| **FastAPI Swagger Docs** | `http://localhost:8000/docs` | `http://<MASTER_IP>:8000/docs` | Interactive REST API explorer |
| **Spark Master UI** | `http://localhost:8080` | `http://<MASTER_IP>:8080` | Displays registered workers & streaming apps |
| **Spark Worker 1 UI** | `http://localhost:8081` | `http://<WORKER1_IP>:8081` | Worker 1 executors and memory metrics |
| **Spark Worker 2 UI** | `http://localhost:8082` | `http://<WORKER2_IP>:8081` | Worker 2 executors and memory metrics |
| **HDFS NameNode UI** | `http://localhost:9870` | `http://<MASTER_IP>:9870` | Storage capacity & Live DataNodes |
| **HBase Master UI** | `http://localhost:16010` | `http://<MASTER_IP>:16010` | RegionServers, load balance & table status |

---

## 6. Teardown Procedure

To cleanly stop all containers and networks:
```bash
# In PowerShell or WSL2:
docker compose -p threatnexus-worker1 -f docker-compose.worker.yml down
docker compose -p threatnexus-worker2 -f docker-compose.worker.yml down
docker compose -p threatnexus-master -f docker-compose.master.yml down
```
*(Add `-v` if you wish to wipe volume state: `docker compose ... down -v`)*


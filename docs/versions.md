# ThreatNexus Technology Version Matrix
Recorded per Phase 1 (Step 1.2) of the implementation guide.

All components have been verified for inter-compatibility across the Hadoop, Spark, Kafka, and HBase ecosystem:

| Component | Target Version | Docker Image / Package | Compatible Runtime | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Java JDK** | 1.8.0_212 (Java 8) | Embedded in base Hadoop & HBase images | JVM 64-bit | Required for Hadoop 3.2.1 and HBase 2.6.5 compatibility |
| **Hadoop HDFS** | 3.2.1 | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`<br/>`bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | Java 8 | Storage layer for raw/curated events & checkpoints |
| **Apache Kafka** | 7.7.1 (Kafka 3.7) | `confluentinc/cp-kafka:7.7.1` | Java 11/17 | High-throughput distributed event streaming broker |
| **Apache ZooKeeper** | 3.9 | `zookeeper:3.9` | Java 11/17 | Cluster coordination for Kafka and HBase |
| **Apache Spark** | 3.5.3 (Scala 2.12) | `bitnamilegacy/spark:3.5.3` | Python 3.10 / Java 17 | Spark Structured Streaming engine |
| **Spark-Kafka Connector**| 3.5.3 (Scala 2.12) | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` | Maven central package | Loaded dynamically in spark-submit |
| **Apache HBase** | 2.6.5 | `openeuler/hbase:2.6.5-oe2403sp3` | Java 8 | NoSQL distributed column store & Thrift service (port 9090) |
| **Python** | 3.10+ | Standard CPython | Host / Container | FastApi 0.115, HappyBase 1.3, Kafka-Python 2.0 |
| **Node.js** | 20 LTS (Alpine) | `node:20-alpine` | V8 | Frontend runtime |
| **Vite** | 6.0.5 | `vite@6.0.5` | Node.js | Fast frontend build tool and dev server |
| **React** | 18.3.1 | `react@18.3.1`, `react-dom@18.3.1` | Browser | Frontend user interface framework |


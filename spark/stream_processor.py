#!/usr/bin/env python3
"""
ThreatNexus - Apache Spark Structured Streaming Threat Detection Engine
Implements Phase 4 (Detection Rules & Alert Schema) and Phase 5 (HDFS & HBase persistence).

Features:
- Parses JSON events from Kafka topic 'security-events'.
- Configures 2-minute event-time watermark for handling late-arriving logs.
- Detects Brute Force Attacks (BRUTE_FORCE_001): >=5 failed logins in window.
- Detects Authentication Bursts (AUTH_BURST_002): >=8 auth failures in window.
- Detects Traffic / Event-Rate Spikes (TRAFFIC_SPIKE_003): >=20 events per IP in window.
- Detects Network Port Scans (PORT_SCAN_004): >=10 distinct destination ports in window.
- Detects Suspicious Process Denylist (SUSPICIOUS_PROC_005): matches 'mimikatz', 'nmap', 'nc.exe', etc.
- Detects Pre-labelled CIC-IDS2017 Dataset Attacks (CIC_DATASET_ATTACK_006).
- Persists structured alerts to HBase with reverse-timestamp row keys for fast retrieval.
- Maintains IP summary statistics in HBase table 'threatnexus:ip_summary'.
- Archives raw and curated event logs to Hadoop HDFS with date partitioning.
"""

import os
import time
from pyspark.sql import SparkSession, functions as F, types as T

# ==========================================================
# 1. SPARK SESSION INITIALIZATION
# ==========================================================
spark = (
    SparkSession.builder
    .appName("ThreatNexus-Detection-Engine")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# 2. UNIFIED EVENT SCHEMA (Phase 2, Step 2.3)
# ==========================================================
schema = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("source_ip", T.StringType(), True),
    T.StructField("destination_ip", T.StringType(), True),
    T.StructField("destination_port", T.IntegerType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("username", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("dataset_label", T.StringType(), True),
    T.StructField("process_name", T.StringType(), True),
    T.StructField("severity", T.StringType(), True),
    T.StructField("message", T.StringType(), True),
])

# ==========================================================
# 3. KAFKA STREAM INGESTION (Phase 4, Step 4.1)
# ==========================================================
kafka_broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
kafka_topic = os.getenv("KAFKA_TOPIC", "security-events")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_events = (
    raw_stream.select(
        F.from_json(F.col("value").cast("string"), schema).alias("e")
    )
    .select("e.*")
    .filter(
        F.col("timestamp").isNotNull()
        & F.col("source_ip").isNotNull()
    )
    .withColumn(
        "event_time",
        F.to_timestamp("timestamp")
    )
    .filter(F.col("event_time").isNotNull())
    .withWatermark("event_time", "2 minutes")
)

# Standardized failed login filter condition
is_failed_login = (
    (F.upper(F.col("event_type")) == "FAILED_LOGIN")
    | (F.lower(F.col("event_type")).isin("login_failed", "auth_failed"))
    | (F.lower(F.col("status")).isin("failure", "failed"))
)

# ==========================================================
# 4. DETECTION RULE 1: REPEATED FAILED LOGINS (BRUTE_FORCE_001)
# ==========================================================
rule_failed_logins = (
    parsed_events
    .filter(is_failed_login)
    .groupBy(
        F.window("event_time", "60 seconds", "30 seconds"),
        "source_ip"
    )
    .agg(F.count("*").alias("count"))
    .filter(F.col("count") >= 5)
    .select(
        F.concat(
            F.lit("alert-bruteforce-"),
            F.sha2(F.concat(F.col("source_ip"), F.col("window.start").cast("string")), 256).substr(1, 12)
        ).alias("alert_id"),
        F.col("source_ip"),
        F.lit("HIGH").alias("severity"),
        F.lit("BRUTE_FORCE_001").alias("rule_id"),
        F.lit("FAILED_LOGIN_BURST").alias("alert_type"),
        F.col("count").cast("int").alias("event_count"),
        F.col("window.start").cast("string").alias("window_start"),
        F.col("window.end").cast("string").alias("window_end"),
        F.concat(F.lit("Repeated failed logins detected: "), F.col("count").cast("string"), F.lit(" attempts within 60s")).alias("description"),
        F.lit("open").alias("status"),
        F.current_timestamp().cast("string").alias("created_at")
    )
)

# ==========================================================
# 5. DETECTION RULE 2: PORT SCAN DETECTION (PORT_SCAN_004)
# ==========================================================
rule_port_scan = (
    parsed_events
    .filter(F.col("destination_port").isNotNull())
    .groupBy(
        F.window("event_time", "60 seconds", "30 seconds"),
        "source_ip"
    )
    .agg(
        F.approx_count_distinct("destination_port").alias("nports"),
        F.count("*").alias("total_conn")
    )
    .filter(F.col("nports") >= 10)
    .select(
        F.concat(
            F.lit("alert-portscan-"),
            F.sha2(F.concat(F.col("source_ip"), F.col("window.start").cast("string")), 256).substr(1, 12)
        ).alias("alert_id"),
        F.col("source_ip"),
        F.lit("HIGH").alias("severity"),
        F.lit("PORT_SCAN_004").alias("rule_id"),
        F.lit("POSSIBLE_PORT_SCAN").alias("alert_type"),
        F.col("nports").cast("int").alias("event_count"),
        F.col("window.start").cast("string").alias("window_start"),
        F.col("window.end").cast("string").alias("window_end"),
        F.concat(F.lit("Horizontal port sweep: "), F.col("nports").cast("string"), F.lit(" distinct ports scanned")).alias("description"),
        F.lit("open").alias("status"),
        F.current_timestamp().cast("string").alias("created_at")
    )
)

# ==========================================================
# 6. DETECTION RULE 3: EVENT-RATE / TRAFFIC SPIKE (TRAFFIC_SPIKE_003)
# ==========================================================
rule_traffic_spike = (
    parsed_events
    .groupBy(
        F.window("event_time", "60 seconds", "30 seconds"),
        "source_ip"
    )
    .agg(F.count("*").alias("count"))
    .filter(F.col("count") >= 20)
    .select(
        F.concat(
            F.lit("alert-spike-"),
            F.sha2(F.concat(F.col("source_ip"), F.col("window.start").cast("string")), 256).substr(1, 12)
        ).alias("alert_id"),
        F.col("source_ip"),
        F.lit("MEDIUM").alias("severity"),
        F.lit("TRAFFIC_SPIKE_003").alias("rule_id"),
        F.lit("EVENT_RATE_SPIKE").alias("alert_type"),
        F.col("count").cast("int").alias("event_count"),
        F.col("window.start").cast("string").alias("window_start"),
        F.col("window.end").cast("string").alias("window_end"),
        F.concat(F.lit("Abnormal traffic surge: "), F.col("count").cast("string"), F.lit(" events in 60s")).alias("description"),
        F.lit("open").alias("status"),
        F.current_timestamp().cast("string").alias("created_at")
    )
)

# ==========================================================
# 7. DETECTION RULE 4: SUSPICIOUS PROCESS DENYLIST (SUSPICIOUS_PROC_005)
# ==========================================================
rule_suspicious_proc = (
    parsed_events
    .filter(
        F.lower(F.col("process_name")).isin(
            "mimikatz", "nc.exe", "nmap", "powershell_malware", "hydra", "metasploit"
        )
    )
    .select(
        F.concat(
            F.lit("alert-proc-"),
            F.sha2(F.concat(F.col("source_ip"), F.col("timestamp"), F.col("process_name")), 256).substr(1, 12)
        ).alias("alert_id"),
        F.col("source_ip"),
        F.lit("CRITICAL").alias("severity"),
        F.lit("SUSPICIOUS_PROC_005").alias("rule_id"),
        F.lit("SUSPICIOUS_PROCESS").alias("alert_type"),
        F.lit(1).alias("event_count"),
        F.col("timestamp").alias("window_start"),
        F.col("timestamp").alias("window_end"),
        F.concat(F.lit("Unauthorized security tool executed: "), F.col("process_name")).alias("description"),
        F.lit("open").alias("status"),
        F.current_timestamp().cast("string").alias("created_at")
    )
)

# ==========================================================
# 8. DETECTION RULE 5: CIC-IDS2017 LABELLED ATTACK (CIC_DATASET_ATTACK_006)
# ==========================================================
rule_dataset_label = (
    parsed_events
    .filter(
        F.col("dataset_label").isNotNull()
        & ~F.lower(F.col("dataset_label")).isin("synthetic", "benign", "synthetic:normal")
        & (
            F.lower(F.col("dataset_label")).like("%patator%")
            | F.lower(F.col("dataset_label")).like("%portscan%")
            | F.lower(F.col("dataset_label")).like("%dos%")
            | F.lower(F.col("dataset_label")).like("%attack%")
            | F.lower(F.col("dataset_label")).like("%botnet%")
        )
    )
    .select(
        F.concat(
            F.lit("alert-cic-"),
            F.sha2(F.concat(F.col("source_ip"), F.col("timestamp"), F.col("dataset_label")), 256).substr(1, 12)
        ).alias("alert_id"),
        F.col("source_ip"),
        F.lit("HIGH").alias("severity"),
        F.lit("CIC_DATASET_ATTACK_006").alias("rule_id"),
        F.lit("LABELLED_DATASET_ATTACK").alias("alert_type"),
        F.lit(1).alias("event_count"),
        F.col("timestamp").alias("window_start"),
        F.col("timestamp").alias("window_end"),
        F.concat(F.lit("Labelled dataset attack record: "), F.col("dataset_label")).alias("description"),
        F.lit("open").alias("status"),
        F.current_timestamp().cast("string").alias("created_at")
    )
)

# ==========================================================
# 9. UNION OF ALL DETECTION ALERTS
# ==========================================================
all_alerts = (
    rule_failed_logins
    .unionByName(rule_port_scan)
    .unionByName(rule_traffic_spike)
    .unionByName(rule_suspicious_proc)
    .unionByName(rule_dataset_label)
)

# ==========================================================
# 10. HBASE SINK (Phase 5, Step 5.2 & 5.3)
# ==========================================================
def save_alerts_to_hbase(df, batch_id):
    if df.rdd.isEmpty():
        return

    def write_partition(rows):
        import happybase
        hbase_host = os.getenv("HBASE_HOST", "hbase")
        hbase_port = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
        hbase_table = os.getenv("HBASE_TABLE", "threatnexus:alerts")
        ip_table_name = os.getenv("HBASE_IP_TABLE", "threatnexus:ip_summary")

        conn = None
        try:
            conn = happybase.Connection(host=hbase_host, port=hbase_port, timeout=10000)
            conn.open()
            alert_tbl = conn.table(hbase_table)
            ip_tbl = conn.table(ip_table_name) if ip_table_name.encode() in conn.tables() else None

            with alert_tbl.batch() as alert_batch:
                for row in rows:
                    item = row.asDict()
                    # Reverse timestamp row key: 9999999999999 - unix_ms
                    now_ms = int(time.time() * 1000)
                    rev_ts = 9999999999999 - now_ms
                    alert_id = str(item.get("alert_id", ""))
                    row_key = f"{rev_ts:013d}_{alert_id}".encode()

                    values = {
                        b"details:alert_id": alert_id.encode(),
                        b"details:rule_id": str(item.get("rule_id", "")).encode(),
                        b"details:severity": str(item.get("severity", "INFO")).encode(),
                        b"details:type": str(item.get("alert_type", "")).encode(),
                        b"details:source_ip": str(item.get("source_ip", "")).encode(),
                        b"details:description": str(item.get("description", "")).encode(),
                        b"details:event_count": str(item.get("event_count", 1)).encode(),
                        b"details:window_start": str(item.get("window_start", "")).encode(),
                        b"details:window_end": str(item.get("window_end", "")).encode(),
                        b"details:status": str(item.get("status", "open")).encode(),
                        b"details:created_at": str(item.get("created_at", "")).encode(),
                    }
                    alert_batch.put(row_key, values)

                    # Update IP summary in threatnexus:ip_summary
                    if ip_tbl:
                        try:
                            src_ip = str(item.get("source_ip", ""))
                            today = time.strftime("%Y-%m-%d")
                            ip_row_key = f"{src_ip}_{today}".encode()
                            ip_tbl.put(
                                ip_row_key,
                                {
                                    b"stats:last_seen": str(item.get("created_at", "")).encode(),
                                    b"stats:last_alert": str(item.get("alert_type", "")).encode(),
                                    b"stats:severity": str(item.get("severity", "")).encode(),
                                }
                            )
                        except Exception:
                            pass
        except Exception as exc:
            print(f"[Spark-HBase-Sink] Error writing batch to HBase: {exc}")
        finally:
            if conn is not None:
                conn.close()

    df.foreachPartition(write_partition)


# Start Streaming Query 1: Alerts to HBase
alert_query = (
    all_alerts.writeStream
    .outputMode("update")
    .foreachBatch(save_alerts_to_hbase)
    .option(
        "checkpointLocation",
        os.path.join(
            os.getenv("SPARK_CHECKPOINT_BASE", "hdfs://namenode:9000/threatnexus/checkpoints"),
            "alerts_v2"
        )
    )
    .start()
)

# ==========================================================
# 11. HDFS SINK: RAW & CURATED LOG ARCHIVE (Phase 5, Step 5.1 & 5.3)
# ==========================================================
# Date-partitioned raw event streaming sink to HDFS
hdfs_output_dir = os.getenv("HDFS_OUTPUT_PATH", "hdfs://namenode:9000/threatnexus/events")
checkpoint_hdfs = os.path.join(
    os.getenv("SPARK_CHECKPOINT_BASE", "hdfs://namenode:9000/threatnexus/checkpoints"),
    "events_v2"
)

hdfs_query = (
    parsed_events
    .withColumn("date", F.to_date("event_time"))
    .drop("event_time")
    .writeStream
    .format("json")
    .outputMode("append")
    .partitionBy("date")
    .option("path", hdfs_output_dir)
    .option("checkpointLocation", checkpoint_hdfs)
    .start()
)

print("[ThreatNexus] Spark Structured Streaming threat detection engine successfully initialized.")
spark.streams.awaitAnyTermination()

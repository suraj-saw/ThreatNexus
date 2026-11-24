#!/usr/bin/env python3
"""
ThreatNexus - Apache Spark Structured Streaming Threat Detection Engine

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
# 2. UNIFIED EVENT SCHEMA
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
# 3. KAFKA STREAM INGESTION
# ==========================================================
kafka_broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
kafka_topic = os.getenv("KAFKA_TOPIC", "security-events")
# Only used on the very first run (no checkpoint yet). "earliest" makes sure events
# published while Spark was still starting up are not silently skipped.
kafka_starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")


def ensure_kafka_topic(bootstrap_servers, topic, attempts=30, delay_s=5):
    """Create the Kafka topic if it does not exist yet.

    Kafka only auto-creates a topic when the first message is produced. If Spark
    subscribes before that, the query dies with UnknownTopicOrPartitionException,
    and every event sent while it restarts is lost. Uses the kafka-clients jar that
    spark-sql-kafka already puts on the driver classpath.
    """
    jvm = spark.sparkContext._jvm
    for attempt in range(1, attempts + 1):
        admin = None
        try:
            props = jvm.java.util.Properties()
            props.put("bootstrap.servers", bootstrap_servers)
            admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
            if topic in set(admin.listTopics().names().get()):
                print(f"[ThreatNexus] Kafka topic '{topic}' is available.")
                return
            new_topic = jvm.org.apache.kafka.clients.admin.NewTopic(
                topic, jvm.java.util.Optional.empty(), jvm.java.util.Optional.empty()
            )
            admin.createTopics(jvm.java.util.Collections.singletonList(new_topic)).all().get()
            print(f"[ThreatNexus] Created Kafka topic '{topic}'.")
            return
        except Exception as exc:
            if "TopicExistsException" in str(exc):
                return
            print(f"[ThreatNexus] Waiting for Kafka topic '{topic}' (attempt {attempt}/{attempts}): {exc}")
            time.sleep(delay_s)
        finally:
            if admin is not None:
                admin.close()
    raise RuntimeError(f"Kafka topic '{topic}' unavailable at {bootstrap_servers}")


ensure_kafka_topic(kafka_broker, kafka_topic)

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", kafka_starting_offsets)
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
        (F.col("window.start").cast("long") * 1000).alias("window_start_ms"),
        (F.col("window.end").cast("long") * 1000).alias("window_end_ms"),
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
        (F.col("window.start").cast("long") * 1000).alias("window_start_ms"),
        (F.col("window.end").cast("long") * 1000).alias("window_end_ms"),
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
        (F.col("window.start").cast("long") * 1000).alias("window_start_ms"),
        (F.col("window.end").cast("long") * 1000).alias("window_end_ms"),
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
        (F.col("event_time").cast("long") * 1000).alias("window_start_ms"),
        (F.col("event_time").cast("long") * 1000).alias("window_end_ms"),
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
        (F.col("event_time").cast("long") * 1000).alias("window_start_ms"),
        (F.col("event_time").cast("long") * 1000).alias("window_end_ms"),
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
# 10. HBASE SINK
# ==========================================================
# Rules evaluated per event (no window): each matching event is one occurrence.
INSTANT_RULES = {"SUSPICIOUS_PROC_005", "CIC_DATASET_ATTACK_006"}
# Instant-rule occurrences from the same IP closer than this are merged into one incident.
INSTANT_MERGE_GAP_MS = 60_000


def save_alerts_to_hbase(df, batch_id):
    """Persist alerts to HBase as de-duplicated incidents.

    A burst of attack events produces several detector outputs: it falls into two
    overlapping 60s sliding windows, update mode re-emits a window each time its count
    grows, and instant rules fire once per event. Writing each output as a new row made
    one attack show up as many alerts, kept the threat gauge pinned at 100, and made
    "Resolve" appear broken (duplicate rows share an alert_id). Instead, outputs for the
    same (rule, source_ip) whose time ranges overlap or touch are merged into a single
    incident row. A merge only happens when new attack events arrived, so a resolved
    incident that receives new events is re-opened.
    """
    if df.rdd.isEmpty():
        return

    def write_partition(rows):
        import happybase
        from datetime import datetime, timezone

        hbase_host = os.getenv("HBASE_HOST", "hbase")
        hbase_port = int(os.getenv("HBASE_THRIFT_PORT", "9090"))
        hbase_table = os.getenv("HBASE_TABLE", "threatnexus:alerts")
        ip_table_name = os.getenv("HBASE_IP_TABLE", "threatnexus:ip_summary")

        def b(value):
            return str(value).encode()

        conn = None
        try:
            conn = happybase.Connection(host=hbase_host, port=hbase_port, timeout=10000)
            conn.open()
            alert_tbl = conn.table(hbase_table)
            ip_tbl = conn.table(ip_table_name) if ip_table_name.encode() in conn.tables() else None

            for row in rows:
                item = row.asDict()
                rule_id = str(item.get("rule_id", ""))
                src_ip = str(item.get("source_ip", ""))
                alert_id = str(item.get("alert_id", ""))
                start_ms = int(item.get("window_start_ms") or time.time() * 1000)
                end_ms = int(item.get("window_end_ms") or start_ms)
                count = int(item.get("event_count") or 1)
                instant = rule_id in INSTANT_RULES
                now_str = str(item.get("created_at", ""))

                # Incident index lives in ip_summary: row {ip}_{date}, column stats:incident_{rule}
                day = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                ip_row_key = f"{src_ip}_{day}".encode()
                index_col = f"stats:incident_{rule_id}".encode()

                prev = None
                if ip_tbl is not None:
                    raw = ip_tbl.row(ip_row_key, columns=[index_col]).get(index_col)
                    if raw:
                        prev_key, prev_end = raw.decode().rsplit("|", 1)
                        prev = (prev_key.encode(), int(prev_end))

                merge_gap = INSTANT_MERGE_GAP_MS if instant else 0
                existing = None
                if prev is not None and start_ms <= prev[1] + merge_gap:
                    existing = alert_tbl.row(prev[0])

                if existing:
                    # Same incident: update it in place instead of adding a duplicate row.
                    row_key = prev[0]
                    old_count = int(existing.get(b"details:event_count", b"1") or 1)
                    new_count = old_count + count if instant else max(old_count, count)
                    new_end = max(prev[1], end_ms)
                    values = {
                        b"details:event_count": b(new_count),
                        b"details:window_end_ms": b(new_end),
                        b"details:updated_at": b(now_str),
                    }
                    if end_ms >= prev[1]:
                        values[b"details:window_end"] = b(item.get("window_end", ""))
                    if instant:
                        values[b"details:description"] = existing.get(b"details:description", b"").split(b" (x")[0] + f" (x{new_count})".encode()
                    elif count >= old_count:
                        values[b"details:description"] = b(item.get("description", ""))
                    if existing.get(b"details:status", b"open") == b"resolved":
                        values[b"details:status"] = b"open"  # new attack events after resolution
                    alert_tbl.put(row_key, values)
                else:
                    # New incident. Row key = reverse event-time + alert_id: newest-first scans,
                    # and deterministic, so a retried micro-batch overwrites instead of duplicating.
                    row_key = f"{9999999999999 - start_ms:013d}_{alert_id}".encode()
                    new_end = end_ms
                    values = {
                        b"details:alert_id": b(alert_id),
                        b"details:rule_id": b(rule_id),
                        b"details:severity": b(item.get("severity", "INFO")),
                        b"details:type": b(item.get("alert_type", "")),
                        b"details:source_ip": b(src_ip),
                        b"details:description": b(item.get("description", "")),
                        b"details:event_count": b(count),
                        b"details:window_start": b(item.get("window_start", "")),
                        b"details:window_end": b(item.get("window_end", "")),
                        b"details:window_start_ms": b(start_ms),
                        b"details:window_end_ms": b(end_ms),
                        b"details:created_at": b(now_str),
                        b"details:updated_at": b(now_str),
                    }
                    if not alert_tbl.row(row_key, columns=[b"details:status"]):
                        values[b"details:status"] = b"open"
                    alert_tbl.put(row_key, values)

                if ip_tbl is not None:
                    ip_tbl.put(ip_row_key, {
                        index_col: row_key + b"|" + b(new_end),
                        b"stats:last_seen": b(now_str),
                        b"stats:last_alert": b(item.get("alert_type", "")),
                        b"stats:severity": b(item.get("severity", "")),
                    })
        except Exception as exc:
            # Re-raise so Spark fails and retries this micro-batch from the checkpoint,
            # instead of silently dropping the alerts (e.g. while HBase is still starting).
            print(f"[Spark-HBase-Sink] Error writing batch to HBase: {exc}")
            raise
        finally:
            if conn is not None:
                conn.close()

    # All outputs of one (rule, IP) go to the same partition, in time order, so incident
    # merging is never raced by another executor; different keys still write in parallel.
    (
        df.repartition("rule_id", "source_ip")
        .sortWithinPartitions("window_start_ms")
        .foreachPartition(write_partition)
    )


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
# 11. HDFS SINK: RAW & CURATED LOG ARCHIVE
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

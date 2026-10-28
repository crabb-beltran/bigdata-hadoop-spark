"""
Spark Structured Streaming consumer for simulated toll events (Kafka topic toll_events).

Same consumer pattern as the course's Anexo 3 (spark_streaming_consumer.py):
read from Kafka, parse JSON, aggregate per time window, print to console.
Additions over the annex:
  - watermark + "update" output mode, so window state does not grow forever;
  - checkpoints in HDFS (shared storage, required by stateful queries);
  - a second query that raises evasion alerts per toll and window.
"""
import argparse

from pyspark.sql import SparkSession, functions as F, types as T

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("peaje", T.StringType()),
    T.StructField("categoria", T.StringType()),
    T.StructField("valor_tarifa", T.IntegerType()),
    T.StructField("evade", T.BooleanType()),
    T.StructField("timestamp", T.TimestampType()),
])


def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("TrafficStreaming")
        .master("spark://spark-master:7077")
        # Few events per window: 200 shuffle partitions would only add overhead.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark, bootstrap, topic, starting_offsets):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .filter(F.col("peaje").isNotNull())  # drop malformed messages
        # Events more than 2 minutes late are ignored and old windows are released.
        .withWatermark("timestamp", "2 minutes")
    )


def traffic_per_toll(events, window):
    return (
        events.groupBy(F.window("timestamp", window), "peaje")
        .agg(
            F.count("*").alias("vehiculos"),
            F.sum(F.when(~F.col("evade"), F.col("valor_tarifa")).otherwise(0)).alias("recaudo"),
            F.sum(F.col("evade").cast("int")).alias("evasores"),
        )
        .withColumn("evasion_pct", F.round(F.col("evasores") * 100.0 / F.col("vehiculos"), 2))
        .select(
            F.date_format("window.start", "HH:mm:ss").alias("inicio"),
            F.date_format("window.end", "HH:mm:ss").alias("fin"),
            "peaje", "vehiculos", "recaudo", "evasores", "evasion_pct",
        )
    )


def start_console_query(df, name, checkpoint, trigger):
    return (
        df.writeStream
        .queryName(name)
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger)
        .start()
    )


def main():
    parser = argparse.ArgumentParser(description="Real-time toll traffic analysis")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", default="toll_events")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    parser.add_argument("--window", default="1 minute")
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--alert-pct", type=float, default=5.0,
                        help="evasion percentage that raises an alert")
    parser.add_argument("--checkpoints", default="hdfs://namenode:8020/data/checkpoints/traffic")
    args = parser.parse_args()

    spark = build_spark_session()
    events = read_events(spark, args.bootstrap, args.topic, args.starting_offsets)
    per_toll = traffic_per_toll(events, args.window)

    # Alerts: windows with enough vehicles and an evasion rate above the threshold.
    # The extra column makes alert batches easy to tell apart in the console.
    alerts = (
        per_toll.filter((F.col("vehiculos") >= 5) & (F.col("evasion_pct") >= args.alert_pct))
        .withColumn("alerta", F.lit("EVASION ALTA"))
    )

    start_console_query(per_toll, "trafico_por_peaje",
                        f"{args.checkpoints}/per_toll", args.trigger)
    start_console_query(alerts, "alertas_evasion",
                        f"{args.checkpoints}/alerts", args.trigger)

    print(f"Streaming from '{args.topic}' | window={args.window} | trigger={args.trigger}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
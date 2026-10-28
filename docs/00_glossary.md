# Glossary

Terms grouped by layer. Each one appears somewhere in the code, the logs or the UIs.

## Storage — Hadoop

| Term | Meaning in this project |
|---|---|
| **HDFS** | Hadoop's distributed file system. Stores the CSVs, the batch outputs and the streaming checkpoints under `/data`. |
| **NameNode** | HDFS master. Keeps the file tree, permissions and *where* each block lives — not the data itself. Address: `hdfs://namenode:8020`. |
| **DataNode** | HDFS worker. Stores the actual blocks. One here, so `dfs.replication=1`. |
| **Block** | Unit HDFS splits files into (128 MB by default). The 14.5 MB traffic CSV fits in a single block. |
| **YARN** | Hadoop's resource manager (`resourcemanager` + `nodemanager`). Part of the stack, but Spark here uses its own standalone manager. |

## Processing — Spark

| Term | Meaning in this project |
|---|---|
| **Spark Master / Worker** | Standalone cluster manager (`spark-master:7077`) and the node that runs executors (`spark-worker`). |
| **Driver** | Process that runs a script's `main()`. In **client mode** it lives inside the `spark-master` container. Serves the UI on `:4040`. |
| **Executor** | JVM started on the worker for one application; runs its tasks. |
| **RDD** | Low-level, immutable, partitioned collection rebuilt from its *lineage* if lost. Used once in the batch job (`map` → `reduceByKey`). |
| **DataFrame** | Distributed table with a schema, optimized by **Catalyst**. Main API of both jobs. |
| **Spark SQL** | SQL over DataFrames; same optimizer and engine. Used for the evasion ranking. |
| **Transformation / Action** | Transformations (`filter`, `groupBy`) only build a plan (**lazy evaluation**); actions (`count`, `show`, `write`) execute it. |
| **Job / Stage / Task** | Each action → a **job**; a job splits into **stages** at every shuffle; a stage runs one **task** per partition. |
| **DAG** | Directed acyclic graph of a job's stages, visible in the `:4040` UI. |
| **Narrow / Wide** | Narrow operations (`filter`, `withColumn`) stay inside a partition; wide ones (`groupBy`, `reduceByKey`) need a **shuffle**. |
| **Shuffle** | Redistribution of rows across partitions by key — the costliest step. Limited to 16 partitions (batch) / 4 (streaming). |
| **Cache** | Keeps the cleaned DataFrame in executor memory so the many actions of the batch job do not re-read HDFS. |

## Messaging — Kafka

| Term | Meaning in this project |
|---|---|
| **Broker** | Kafka server (`kafka` container). One broker, so replication factor 1. |
| **Topic** | Named stream of events: `toll_events`. |
| **Partition** | Ordered, append-only log; a topic is split into partitions for parallelism. `toll_events` has 3. |
| **Offset** | Position of an event inside a partition. Spark records the offsets it has processed in its checkpoint. |
| **Producer** | Writes events: `toll_producer.py`. |
| **Consumer** | Reads events: the Spark streaming job. |
| **Key** | Field used to choose the partition (hash of the key). Here the toll name, so each toll's events stay ordered in one partition. |
| **Listener** | Network endpoint of the broker. `INTERNAL` (`kafka:29092`) for containers, `EXTERNAL` (`localhost:9092`) for the host. |
| **KRaft / ZooKeeper** | Cluster-metadata management. KRaft is built into Kafka; ZooKeeper is the older external coordinator (removed in Kafka 4.0). This stack uses KRaft. |

## Streaming — Spark Structured Streaming

| Term | Meaning in this project |
|---|---|
| **Structured Streaming** | Spark API that treats a stream as an unbounded table and runs the same DataFrame operations on it. |
| **Micro-batch** | Each `Batch: N` in the console: the events that arrived since the previous one, processed as a small batch job. |
| **Trigger** | How often a micro-batch starts: every 10 seconds. |
| **Event time** | The `timestamp` written by the producer (when the vehicle passed), as opposed to processing time. |
| **Window** | Time bucket for aggregation: 1 minute of event time. |
| **Watermark** | "Events more than 2 minutes older than the newest one seen are late." Lets Spark finalize windows and drop their state. |
| **Output mode** | Which rows a micro-batch prints: `complete` (whole table), `update` (changed rows — used here), `append` (finalized rows only). |
| **State store** | Running aggregates kept between micro-batches (counts and sums per window and toll). |
| **Checkpoint** | Offsets + state persisted in HDFS (`/data/checkpoints/traffic`) so the query can resume after a restart. |

## Infrastructure

| Term | Meaning in this project |
|---|---|
| **Docker Compose** | Defines and starts the 7 services from `docker-compose.yml`. |
| **Named volume** | Docker-managed storage that survives container recreation: HDFS metadata and blocks. |
| **Bind mount** | Host folder mapped into a container: `./scripts`, `./data`, `./reports`. Edits on the host are visible immediately. |
| **Pseudo-distributed** | Every daemon runs as a separate process (container) on one machine — real protocols, no real fault tolerance. |
# Big Data Pipeline: HDFS + Spark on Docker

A containerized, pseudo-distributed Big Data stack (Hadoop HDFS + Apache Spark) for
batch analysis of tabular datasets, built as a reproducible alternative to manual
VM-based Hadoop installations.

**Case study dataset**: [Tarifas de Peajes ANI](https://www.datos.gov.co/Transporte/Tarifas-de-Peajes-ANI/7gj8-j6i3) —
toll rates by vehicle category across Colombia's national road concessions
(datos.gov.co, Agencia Nacional de Infraestructura).

## Glossary

Key terms used throughout this project, for quick reference before diving into
the architecture or the walkthrough.

| Term | Meaning |
|---|---|
| **Big Data** | Datasets whose volume, velocity, or variety exceed what traditional single-machine tools can efficiently capture, store, or process. |
| **HDFS** (Hadoop Distributed File System) | Hadoop's distributed storage layer. Splits files into blocks and replicates them across multiple nodes for fault tolerance and parallel access. |
| **NameNode** | HDFS's master node. Holds the filesystem's metadata (directory tree, block locations, permissions) but not the data itself. |
| **DataNode** | HDFS's worker node. Stores the actual data blocks and serves read/write requests from clients. |
| **YARN** (Yet Another Resource Negotiator) | Hadoop's resource manager. Allocates cluster CPU/memory to applications (MapReduce, Spark, etc.) independently of the storage layer. |
| **ResourceManager** | YARN's master process. Tracks cluster-wide resource availability and grants containers to applications. |
| **NodeManager** | YARN's per-node agent. Monitors local resource usage and reports back to the ResourceManager. |
| **MapReduce** | Hadoop's original distributed processing model: a *Map* phase transforms data in parallel, a *Reduce* phase aggregates the results. Superseded in this project by Spark, but still part of the underlying Hadoop ecosystem. |
| **Apache Spark** | In-memory distributed processing engine. Faster than MapReduce for iterative workloads because it avoids writing intermediate results to disk. |
| **Spark Master** | Coordinates a Spark standalone cluster: tracks connected workers and assigns resources to submitted applications. |
| **Spark Worker** | A node that executes tasks assigned by the Spark Master, running one or more executors. |
| **Executor** | A JVM process launched by a worker to run tasks for a specific Spark application; holds data in memory/disk for that app's lifetime. |
| **Driver** | The process running the `main()` of a Spark application (here, `analyze.py`). Coordinates executors and collects results. In **client mode** (used in this project), the driver runs on the machine that launched `spark-submit` — here, inside the `spark-master` container. |
| **DataFrame** | Spark's primary structured data abstraction: a distributed table with named columns, similar to a pandas or SQL table. |
| **PySpark** | The Python API for Apache Spark, used to write `analyze.py`. |
| **`spark-submit`** | The command-line tool used to launch a Spark application against a cluster. |
| **Pseudo-distributed mode** | A cluster configuration where all Hadoop/Spark daemons run as separate processes (or, here, containers) but on a single physical machine — used for learning and development, not production scale. |
| **Docker Compose** | Tool for defining and running multi-container Docker applications from a single YAML file (`docker-compose.yml`), used here to orchestrate the 6-service cluster. |
| **Container orchestration** | The general practice of automating the deployment, networking, and lifecycle of multiple containers as a coherent system — what Docker Compose does at small scale. |
| **Web UI / RPC port** | Two categories of ports exposed by this stack: **Web UI** ports serve a browser-based dashboard (e.g. `9870`, `8080`); **RPC** ports are used for internal process-to-process communication and have no browser interface (e.g. `8020`, `7077`). |

## Architecture

```
┌──────────────────────┐     ┌──────────────────┐     ┌───────────────────────┐
│ namenode               │────▶│ datanode          │     │ resourcemanager        │
│ 9870 → HDFS Web UI     │     │ (internal only)   │     │ 8088 → YARN Web UI     │
│ 8020 → HDFS RPC        │     │                   │     │                        │
└──────────────────────┘     └──────────────────┘     └───────────────────────┘
           │                                                      │
           │                                             ┌──────────────────┐
           │                                             │ nodemanager       │
           │                                             │ (internal only)   │
           │                                             └──────────────────┘
           ▼
┌──────────────────────┐     ┌──────────────────┐
│ spark-master            │────▶│ spark-worker      │
│ 8080 → Spark Master UI  │     │ (internal only)   │
│ 7077 → Spark RPC        │     │                   │
│ 4040 → App UI (driver,  │     │                   │
│         only while a    │     │                   │
│         job is running) │     │                   │
└──────────────────────┘     └──────────────────┘
```

5 services, single-node pseudo-distributed cluster: `namenode`, `datanode`,
`resourcemanager`, `nodemanager` (Hadoop 3.2.1), `spark-master`, `spark-worker`
(Apache Spark 3.5.3).

## Web UIs and ports

| Port | Service | Purpose |
|---|---|---|
| **9870** | HDFS NameNode Web UI | Browse the distributed filesystem, check block/node health, capacity. |
| **8020** | HDFS RPC (NameNode) | Internal port for client↔NameNode communication (`hdfs://namenode:8020/...`). No web UI. |
| **8088** | YARN ResourceManager Web UI | Monitor submitted applications, cluster resource usage, queues. |
| **7077** | Spark Master (RPC) | Endpoint workers and `spark-submit` connect to (`spark://spark-master:7077`). No web UI. |
| **8080** | Spark Master Web UI | View connected workers, running/completed applications, available cores/memory. |
| **4040** | Spark Application UI (driver) | **Only exists while a Spark application is actively running.** Shows jobs, stages, tasks and the execution DAG for that specific app; disappears when the job finishes. |

All six ports must be mapped in `docker-compose.yml` under the corresponding
service's `ports:` section — it's easy to forget `4040` since Spark logs confirm
it starts internally even when it isn't exposed to the host (see Troubleshooting).

## Prerequisites

- Docker Engine + Docker Compose v2
- ~4 GB RAM available for containers
- A CSV dataset (or use the included toll rates sample)

## Quick start

```bash
git clone <this-repo>
cd bigdata-hadoop-spark

docker compose up -d
docker compose ps          # wait until namenode shows "healthy"
```

Load a dataset into HDFS:

```bash
chmod +x scripts/load_to_hdfs.sh
./scripts/load_to_hdfs.sh data/peajes_ani.csv /data
```

Run the analysis job:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /scripts/analyze.py \
  --input hdfs://namenode:8020/data/peajes_ani.csv \
  --output hdfs://namenode:8020/data/output/high_value \
  --output-agg hdfs://namenode:8020/data/output/by_category
```

> Note: the official `apache/spark` image does **not** add `spark-submit` to
> `$PATH` (unlike Bitnami's images), so the full path
> `/opt/spark/bin/spark-submit` is required.

## Video evidence checklist

Suggested run-through for recording a walkthrough video of this pipeline
(target: under 5 minutes), in the order that best demonstrates the underlying
architecture rather than just the final output:

1. **Cluster startup** — `docker compose up -d` and `docker compose ps` showing
   all 6 services `healthy`/`running`.
2. **HDFS Web UI (`localhost:9870`)** — Overview with active NameNode; Utilities
   → Browse the file system down to the loaded dataset.
3. **YARN Web UI (`localhost:8088`)** — cluster metrics, active nodes (brief).
4. **Load to HDFS** — run `load_to_hdfs.sh` live, showing the `mkdir` / `put` /
   `ls` steps against the real cluster.
5. **Spark Master UI (`localhost:8080`)** — 1 worker `ALIVE`, available
   cores/memory before submitting any job.
6. **Run the analysis** — `spark-submit` executing live: schema, sample rows,
   `summary()`, filter, sorted top 10, and the category `groupBy`/aggregation.
7. **Spark Application UI (`localhost:4040`)** — while the job is paused (see
   note below), show the Jobs tab and at least one stage's DAG visualization.
8. **Result verification** — `hdfs dfs -ls /data/output/` or the HDFS web UI,
   confirming `high_value` and `by_category` were persisted.
9. **Closing note** — briefly state that Docker was used as the equivalent
   environment to the course's VirtualBox instructions, per the justification
   documented below.

**Tip for step 7**: since port 4040 only serves while a `SparkContext` is alive,
add a temporary blocking call before `spark.stop()` in `analyze.py`:
```python
input("Press Enter to stop Spark...")  # remove before final commit
spark.stop()
```
This holds the driver open so the UI can be inspected/recorded at leisure.
Only run **one** `spark-submit` at a time against this cluster — with 16 cores
and a single worker, a second job will sit in `WAITING` state until the first
one releases its executors.

## Project structure

```
.
├── docker-compose.yml
├── config/
│   └── hadoop.env          # Hadoop cluster environment variables
├── data/                   # local CSVs (gitignored, except .gitkeep)
├── scripts/
│   ├── load_to_hdfs.sh     # generic HDFS loader (any CSV)
│   └── analyze.py          # generic PySpark analysis job
└── README.md
```

## `analyze.py` — what it does

Generic PySpark batch job over any CSV loaded into HDFS:
1. Infers schema and prints it (`printSchema`).
2. Shows sample rows and descriptive statistics (`summary()`).
3. Filters rows above a numeric threshold (default: `valor > 10000`).
4. Sorts by that column descending (top 10).
5. Groups by a categorical column and computes count/avg/max/min.
6. Persists filtered and aggregated results back to HDFS as CSV.

Column names (`valor`, `idcategoriatarifa`, etc.) are dataset-specific — adjust the
`--input`/`--output` args and the hardcoded column names in `run_analysis()` for a
different dataset.

### Dataset dictionary — `idcategoriatarifa`

| Code | Meaning |
|---|---|
| I | Cars, jeeps, pickups, minibuses (single-tire axles) |
| II | Buses, minibuses (dual rear axle), two-axle trucks |
| III | Trucks and passenger vehicles with 3–4 axles |
| IV | Five-axle trucks |
| V | Six-axle trucks |
| VI–VII | Larger trucks (up to 7+ axles, varies by concession) |
| IE, IE1, IE2, IE10 | Category I with special/discounted rate (regional exemptions) |
| IIE, IIIE, IVE, VE, VIE | Categories II–VI with special rate |
| EA, EC, EG, ER | Additional-axle / sugarcane-axle / concession-specific categories |
| E1-*, E2-* | Special tariff types 1/2 applied over categories III–VII, specific to individual concessions |

> The "special" categories (E, E1, E2 suffixes) don't have a single nationally
> standardized definition — they vary by Ministry of Transport resolution and by
> specific road concession. This is documented here as a known limitation of the
> dataset rather than resolved with an invented mapping.

## Design decisions & trade-offs

- **`bde2020/hadoop-*` images** instead of building Hadoop from scratch: these are
  the de facto standard for pseudo-distributed Hadoop-in-Docker demos. They're
  unmaintained since ~2021 (Hadoop 3.2.1) — acceptable for a learning/portfolio
  project, **not** a choice for production.
- **`apache/spark` (official) instead of `bitnami/spark`**: Bitnami discontinued
  free public tags on Docker Hub in 2025, moving Debian-based images behind a
  paid "Secure Images" tier. The official Apache image has no bundled entrypoint
  logic for master/worker mode selection, so `command:` is set explicitly per
  service instead of relying on a `SPARK_MODE` env var, and `spark-submit` must
  be invoked by full path since it isn't on `$PATH`.
- **Docker instead of VirtualBox**: the course guide's Anexos 1/2 describe a
  manual VirtualBox + Ubuntu Server + PuTTY setup. This project replicates the
  same pseudo-distributed HDFS + Spark architecture and the same analysis
  workflow (dataset → HDFS → PySpark) using containers instead, for
  reproducibility and portfolio purposes.
- **`dfs.replication=1`**: single datanode, so replication above 1 would just fail
  silently to achieve real redundancy. In a real multi-node deployment this
  would be 3 (the HDFS default) at minimum.
- **No persistent object storage**: this stack keeps HDFS as the storage layer to
  match the course's learning objective. In a production data platform, S3 (or
  GCS) + a table format (Iceberg/Delta) would replace HDFS entirely — Hadoop's
  local-disk model doesn't survive container/node loss without the volumes
  shown here, and doesn't decouple storage from compute.

## Known limitations

- Single-node cluster: no fault tolerance testing (kill-a-datanode scenarios),
  no NameNode HA (single point of failure by design here).
- No monitoring/alerting stack (Prometheus/Grafana) — logs only via
  `docker compose logs <service>`.
- No idempotency guard on `load_to_hdfs.sh`: re-running it overwrites the file at
  the destination (`hdfs dfs -put -f`), which is intentional for iteration but
  would need versioning/dedup logic in a real ingestion pipeline.
- HDFS write permissions on `/data` are opened to `777` after load (see
  Troubleshooting below) — acceptable for a single-user practice cluster with no
  Kerberos auth, but not a pattern to carry into a shared/production environment.
- Special toll rate categories (see dictionary above) aren't fully documented in
  a single authoritative public source.

## Troubleshooting

**`docker compose up` fails with "undefined volume"**
Check that `volumes:` is declared as a top-level key (same indentation as
`services:`), not nested inside a service.

**`namenode` never becomes healthy**
```bash
docker compose logs namenode
```
Usually a stale `hadoop_namenode` volume from a previous run with a different
`CLUSTER_NAME`. Reset with:
```bash
docker compose down -v
docker compose up -d
```

**`OCI runtime exec failed ... "spark-submit": executable file not found in $PATH`**
The official `apache/spark` image doesn't add `/opt/spark/bin` to `$PATH`. Use the
full path instead:
```bash
docker exec spark-master /opt/spark/bin/spark-submit ...
```
Confirm the binary location if needed:
```bash
docker exec spark-master ls /opt/spark/bin/
docker exec spark-master env | grep SPARK_HOME
```

**`./scripts/load_to_hdfs.sh: Permission denied` (from the shell, not HDFS)**
The script lost its executable bit (common after copy/paste or a fresh clone).
Fix with:
```bash
chmod +x scripts/load_to_hdfs.sh
```

**PySpark job fails writing output: `AccessControlException: Permission denied: user=spark, access=WRITE, inode="/data"`**
The dataset was loaded into HDFS as `root` (via `docker exec namenode ...`), so
`/data` ends up owned by `root:supergroup` with `755` permissions. But the Spark
driver process runs internally as user `spark`, which then can't create the output
subdirectory. Fix by opening write access on the working directory:
```bash
docker exec namenode hdfs dfs -chmod -R 777 /data
```
`load_to_hdfs.sh` runs this automatically after every load, so this should only
surface if you create HDFS directories manually outside the script.

**`localhost:4040` shows "connection refused" or nothing loads**
Check, in order:
1. **The job already finished.** Port 4040 hosts the Spark *Application* UI, which
   only exists while a `SparkContext` is alive — it's created when the job starts
   and torn down when it calls `spark.stop()`. See the video checklist above for
   how to hold the context open for inspection.
2. **The port isn't published.** Confirm `4040:4040` is present under
   `spark-master`'s `ports:` in `docker-compose.yml` — Spark's logs confirm
   `SparkUI` starts internally even when it's not exposed to the host, which is
   misleading. Verify with:
   ```bash
   docker ps --filter "name=spark-master" --format "{{.Ports}}"
   ```
3. **A `docker compose restart` doesn't apply `ports:` changes to an existing
   container** — a `restart` only restarts the process inside the container, it
   does not re-read port mappings from the compose file. If you changed `ports:`
   after the container was created, recreate it instead:
   ```bash
   docker compose stop spark-master spark-worker
   docker compose rm -f spark-master spark-worker
   docker compose up -d spark-master spark-worker
   ```

**A previous job is stuck `RUNNING` and a new one sits in `WAITING` with 0 cores**
Only one `spark-submit` should run at a time against this single-worker cluster.
If a prior job (e.g. one paused on the `input()` from the video checklist) is
holding all cores, either return to that terminal and press Enter, or find and
kill the driver process directly inside the container:
```bash
docker exec spark-master ps aux | grep java
docker exec spark-master kill -9 <PID>
```
Killing the app from the Spark Master UI ("kill" link) or via
`deploy.Client kill` only works for **cluster-mode** submissions — this project
runs in the default **client mode**, where the driver lives inside the
`spark-master` container itself, so the master has no authority to stop it.
If cores still show as used after killing the process, restart both services:
```bash
docker compose restart spark-master spark-worker
```

**Spark job can't resolve `spark-master` / `namenode` hostnames**
Confirm you're running `spark-submit` *inside* the `spark-master` container
(`docker exec spark-master ...`), not from the host — Docker's internal DNS only
resolves service names within the compose network.

## License

MIT (or adjust to your preference).
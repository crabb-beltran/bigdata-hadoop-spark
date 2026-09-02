# Big Data Pipeline: HDFS + Spark on Docker

A containerized, pseudo-distributed Big Data stack (Hadoop HDFS + Apache Spark) for
batch analysis of tabular datasets, built as a reproducible alternative to manual
VM-based Hadoop installations.

**Case study dataset**: [Tarifas de Peajes ANI](https://www.datos.gov.co/Transporte/Tarifas-de-Peajes-ANI/7gj8-j6i3) —
toll rates by vehicle category across Colombia's national road concessions
(datos.gov.co, Agencia Nacional de Infraestructura).

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────────┐
│  namenode   │────▶│  datanode   │     │ resourcemanager   │
│  (HDFS UI)  │     │  (storage)  │     │  (YARN UI)        │
└─────────────┘     └─────────────┘     └──────────────────┘
       │                                          │
       │                                   ┌──────────────┐
       │                                   │ nodemanager  │
       │                                   └──────────────┘
       ▼
┌─────────────┐     ┌─────────────┐
│ spark-master│────▶│ spark-worker│
│  (Spark UI) │     │             │
└─────────────┘     └─────────────┘
```

5 services, single-node pseudo-distributed cluster: `namenode`, `datanode`,
`resourcemanager`, `nodemanager` (Hadoop 3.2.1), `spark-master`, `spark-worker`
(Apache Spark 3.5.3).

## Prerequisites

- Docker Engine + Docker Compose v2
- ~4 GB RAM available for containers
- A CSV dataset (or use the included toll rates sample)

## Quick start

```bash
git clone <this-repo>
cd bigdata-hadoop-spark-trm

docker compose up -d
docker compose ps          # wait until namenode shows "healthy"
```

Load a dataset into HDFS:

```bash
./scripts/load_to_hdfs.sh data/peajes_ani.csv /data
```

Run the analysis job:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /scripts/analyze.py \
  --input hdfs://namenode:8020/data/peajes_ani.csv \
  --output hdfs://namenode:8020/data/output/high_value \
  --output-agg hdfs://namenode:8020/data/output/by_category
```

## Web UIs

| Service | URL | Purpose |
|---|---|---|
| HDFS NameNode | http://localhost:9870 | Browse filesystem, check block health |
| YARN ResourceManager | http://localhost:8088 | Monitor cluster resource allocation |
| Spark Master | http://localhost:8080 | View running/completed jobs, worker status |
| Spark Application UI | http://localhost:4040 | Per-job stage/task details (while a job runs) |

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

## Design decisions & trade-offs

- **`bde2020/hadoop-*` images** instead of building Hadoop from scratch: these are
  the de facto standard for pseudo-distributed Hadoop-in-Docker demos. They're
  unmaintained since ~2021 (Hadoop 3.2.1) — acceptable for a learning/portfolio
  project, **not** a choice for production.
- **`apache/spark` (official) instead of `bitnami/spark`**: Bitnami discontinued
  free public tags on Docker Hub in 2025, moving Debian-based images behind a
  paid "Secure Images" tier. The official Apache image has no bundled entrypoint
  logic for master/worker mode selection, so `command:` is set explicitly per
  service instead of relying on a `SPARK_MODE` env var.
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

**Spark job can't resolve `spark-master` / `namenode` hostnames**
Confirm you're running `spark-submit` *inside* the `spark-master` container
(`docker exec spark-master ...`), not from the host — Docker's internal DNS only
resolves service names within the compose network.

## License

MIT (or adjust to your preference).
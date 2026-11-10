# Multi-Query Optimization in Apache Flink

This project investigates multi-query optimization in Apache Flink:

> When does Flink recognize common computation across multiple streaming queries, what prevents reuse, and what performance benefit does reuse provide?

Queries come from the Nexmark benchmark. They read events from Kafka and run on a Flink session cluster.

## Pinned versions

| Component | Version |
|---|---|
| Flink | 2.2.1 (`flink:2.2.1-scala_2.12-java17`). 2.3.0 has no Kafka connector release yet. |
| Kafka SQL connector | `flink-sql-connector-kafka` 5.0.0-2.2 |
| Kafka | `apache/kafka:4.3.2` (single KRaft node) |
| Python | 3.14 |
| Nexmark | [`6b3646c3baec701f1fa74baf938d235f742e5d3c`](https://github.com/nexmark/nexmark/commit/6b3646c3baec701f1fa74baf938d235f742e5d3c) (submodule in `nexmark/`) |

## Usage

Requires Docker with Compose v2 and `make`. Clone with `git clone --recursive` (or run `git submodule update --init` afterwards).

Everything under `infra/` is driven through the Makefile:

```
make help    # list targets
make up      # start Kafka, Flink (jobmanager, taskmanager) and the SQL gateway
make smoke   # end-to-end check: Flink SQL writes to Kafka and reads it back
make sql     # interactive Flink SQL client
make down    # stop everything and delete all data
```

SQL is submitted through the Flink SQL Gateway, which runs every statement as a job on the session cluster.

| Endpoint | Address |
|---|---|
| Flink web UI / REST | http://localhost:8081 |
| Flink SQL Gateway REST | http://localhost:8083 |
| Kafka (inside compose) | `kafka:9092` |

### Configuration

Flink and Kafka sizing is set in `infra/defaults.env`, which holds defaults for local development. To change values on one machine, create `.env` in the repo root with just the lines to override:

```
TM_COUNT=4
TM_MEMORY=8g
PARALLELISM=4
KAFKA_PARTITIONS=4
```

Single runs can also override on the command line: `make up PARALLELISM=2`. Re-run `make up` after changing values.

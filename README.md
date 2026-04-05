# Databricks Data Engineering — Hands-On Refresh

A structured, hands-on practice repository covering core Databricks Data Engineering
concepts built in preparation for a technical interview.

All notebooks are built on **Databricks Free Edition using 
Serverless Compute (versionless — always latest runtime)**, with production-equivalent
patterns documented inline via code comments.

---

## Repository Structure
```
Databricks-DE-Refresh/
│
├── day1_structured_streaming/        # Spark Structured Streaming — 3 notebooks
│   ├── 01_streaming_basics           # Rate source → Delta sink, checkpoint, time travel
│   ├── 02_watermarking               # Event time, tumbling windows, late data handling
│   └── 03_foreach_batch_merge        # foreachBatch + MERGE upsert, ACID verification
│
├── day1_dlt_lakeflow/                # Delta Live Tables (Lakeflow) — 2 notebooks
│   ├── 01_bronze_silver_gold_pipeline  # Medallion architecture, 3 expectation types
│   └── 02_cdc_apply_changes          # CDC with apply_changes(), SCD Type 1 & 2
│
├── day2_delta_internals/             # Delta Lake internals — [In Progress]
│   ├── 01_delta_operations           # OPTIMIZE, ZORDER, VACUUM, DESCRIBE HISTORY
│   ├── 02_merge_and_cdf              # Change Data Feed, table_changes(), MERGE
│   ├── 03_time_travel_restore        # Bad write recovery, RESTORE TABLE, version pinning
│   └── 04_delta_vs_iceberg           # Format comparison, Delta UniForm, decision framework
│
└── pipeline_configs/                 # DLT pipeline definitions as infrastructure-as-code
    ├── medallion_pipeline.yaml       # Bronze→Silver→Gold pipeline configuration
    ├── cdc_pipeline.yaml             # CDC apply_changes() pipeline configuration
    └── README.md                     # Pipeline setup and recreation instructions
```

---

## Concepts Covered

### Spark Structured Streaming
- `readStream` / `writeStream` with Delta as a sink
- Checkpoint-based exactly-once semantics — fault tolerance and restart recovery
- `trigger(availableNow=True)` — modern replacement for deprecated `trigger(once=True)`
- Watermarking with `withWatermark()` for bounded state and late data handling
- Event time vs processing time — tumbling window aggregations
- `foreachBatch()` pattern — streaming MERGE upserts, multi-sink writes
- ACID verification via `DESCRIBE HISTORY` and Delta transaction log

### Delta Live Tables — Lakeflow Declarative Pipelines
- **Lakeflow** architecture: Lakeflow Connect → Lakeflow Declarative Pipelines → Lakeflow Jobs
  *(DLT was renamed as part of the Lakeflow platform at Databricks Data+AI Summit 2025)*
- Medallion Architecture: Bronze (raw) → Silver (validated) → Gold (aggregated)
- Three DLT expectation types:
  - `@dlt.expect` — flag violations, keep row
  - `@dlt.expect_or_drop` — drop failing rows silently
  - `@dlt.expect_or_fail` — fail entire pipeline on violation
- `dlt.read_stream()` vs `dlt.read()` — incremental vs full batch reads
- Pipeline DAG visualisation and event log observability
- `apply_changes()` for declarative CDC:
  - `sequence_by` for out-of-order event resolution
  - `apply_as_deletes` for delete handling
  - SCD Type 1 (current state only)
  - SCD Type 2 (`stored_as_scd_type=2`) — full history with `__START_AT` / `__END_AT`

### Delta Lake Internals *(In Progress)*
- OPTIMIZE — small file compaction
- ZORDER BY — multi-dimensional clustering for query acceleration
- VACUUM — storage reclamation with retention window
- Change Data Feed (CDF) — `delta.enableChangeDataFeed`, `table_changes()`
- MERGE INTO — upsert patterns outside DLT context
- Time travel — `versionAsOf`, `timestampAsOf`, `RESTORE TABLE`
- Delta vs Apache Iceberg — format comparison and `Delta UniForm`

---

## Environment and Key Decisions

| Item | Community Edition Choice | Production Equivalent |
| Compute | Serverless (auto-managed, versionless) | Classic cluster with pinned DBR version e.g. 15.4 LTS |
| Streaming source | `rate()` built-in | Auto Loader (`cloudFiles`) from ADLS/S3 |
| Pipeline config | YAML in `pipeline_configs/` | Databricks Asset Bundles (DABs) for CI/CD |

> All production equivalents are documented inline in each notebook via comments,
> so the notebooks demonstrate both free-tier execution and production-grade reasoning.

---

## Progress

| Area | Status | Notebooks |
|---|---|---|
| Spark Structured Streaming | ✅ Complete | 3 / 3 |
| DLT / Lakeflow Pipelines | ✅ Complete | 2 / 2 + 2 pipeline configs |
| Delta Lake Internals | 🔄 In Progress | 0 / 4 |
| Delta vs Iceberg | 🔄 In Progress | 0 / 1 |

---

## Key Interview Topics Demonstrated

**Scenario: Pipeline wrote bad data overnight — how do you recover?**
> `DESCRIBE HISTORY` → identify bad version →
> `RESTORE TABLE my_table TO VERSION AS OF N` →
> re-run pipeline from that checkpoint

**Scenario: Streaming job falling behind — how do you diagnose?**
> Spark UI → stages tab → task skew → tune `maxFilesPerTrigger` →
> check watermark lag → scale cluster only after inefficiencies fixed

**Scenario: Delta vs Iceberg — which do you choose?**
> Delta if Databricks-native (Photon, DLT, Unity Catalog, CDF all native).
> Iceberg if multi-engine reads needed (Trino, Snowflake, Flink) or
> avoiding vendor lock-in. Delta UniForm bridges both — Delta tables
> readable as Iceberg by external engines.

**Scenario: SCD Type 1 vs Type 2 — when do you use each?**
> SCD1 for current-state-only needs (product price, stock level).
> SCD2 for full audit history and point-in-time queries —
> `apply_changes(stored_as_scd_type=2)` in DLT handles this declaratively.

---

## How to Run

1. Import this repo into Databricks via **Repos** (left sidebar → Repos → Add Repo)
2. Attach notebooks to a cluster running **Runtime 15.x LTS**
3. Run notebooks in `day1_structured_streaming/` directly on the cluster
4. For DLT notebooks in `day1_dlt_lakeflow/`:
   - Go to **Delta Live Tables → Create Pipeline**
   - Reference pipeline settings from `pipeline_configs/*.yaml`
   - Click **Start** — do not run DLT notebooks directly on a cluster
5. Day 2 notebooks (`day2_delta_internals/`) run directly on the cluster

---

## Author

**Sathiya** — Data Engineer - Databricks Certified Data Engineering Professional / AI Engineering.
Background: Python · PySpark · Apache Airflow · Pinecone (Vector DBs)

 **This repository is built as structured interview preparation,
 with each notebook designed to demonstrate both working code
 and production-grade engineering reasoning.**

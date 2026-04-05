# Databricks notebook source
# Databricks notebook source
# =============================================================================
# NOTEBOOK   : 01_bronze_silver_gold_pipeline
# FOLDER     : day1_dlt_lakeflow
# REPO       : Databricks-DE-Refresh
# PURPOSE    : Demonstrates a full Medallion Architecture pipeline using
#              Delta Live Tables (DLT) — now called Lakeflow Declarative
#              Pipelines in Databricks as of mid-2025.
#
# MEDALLION ARCHITECTURE:
#   BRONZE  → Raw ingestion, no transformation, append-only
#   SILVER  → Cleaned, validated, business rules applied
#   GOLD    → Aggregated metrics ready for BI / reporting
#
# COMMUNITY EDITION NOTES:
#   - Uses rate() streaming source (no Kafka/S3 needed)
#   - Storage managed by DLT pipeline config (no external paths)
#   - No Unity Catalog — uses hive_metastore (default in free tier)
#
# PRODUCTION EQUIVALENT:
#   - Bronze source would be Auto Loader reading from ADLS Gen2 or S3
#   - Storage paths would point to Unity Catalog external locations
#   - Pipeline would be orchestrated via Lakeflow Jobs (Databricks Workflows)
#
# HOW TO RUN:
#   DO NOT run this notebook directly on a cluster.
#   Step 1: Go to Delta Live Tables (left sidebar) → Create Pipeline
#   Step 2: Point "Source Code" to this notebook
#   Step 3: Set Target schema to: dlt_demo
#   Step 4: Set Pipeline mode to: Triggered
#   Step 5: Click Start — watch the DAG graph appear
# =============================================================================

# COMMAND ----------

# Import the DLT library
# In newer Databricks runtimes (Lakeflow SDP), you can also use:
#   from pyspark.sql import pipelines as dp
# But import dlt still works and is more widely recognised in interviews
import dlt
from pyspark.sql.functions import col, current_timestamp, expr, sum as _sum, count

# =============================================================================
# BRONZE LAYER
# =============================================================================
# Rules for Bronze:
#   - Ingest raw data AS-IS — no business logic here
#   - Always streaming (append-only)
#   - Keep all columns including bad/null data
#   - Add metadata columns: ingestion timestamp, source system
#
# Interview point: "Why no quality checks in Bronze?"
# Answer: Bronze is your source of truth. If you drop data here,
# you can never recover it. Apply quality at Silver where you
# have full visibility of what was dropped and why.
# =============================================================================

@dlt.table(
    name="bronze_orders",
    comment="""
        RAW ingestion layer — orders arriving from the source system.
        No transformations applied. Append-only streaming table.
        In production: source would be Auto Loader reading JSON/Parquet
        files landing in ADLS Gen2 or S3.
    """,
    table_properties={
        "quality": "bronze",                    # custom tag — visible in UI
        "pipelines.autoOptimize.managed": "true" # let DLT manage file compaction
    }
)
def bronze_orders():
    """
    Reads from the rate() source — generates sequential integers at a fixed rate.
    We shape this into a realistic orders schema using column expressions.

    rate() produces two columns:
      - timestamp : event time (we use this as order_time)
      - value     : monotonically increasing long (we use this to derive other cols)
    """
    return (
        spark.readStream
        .format("rate")                         # built-in source, no setup needed
        .option("rowsPerSecond", 5)             # 5 new orders per second
        .load()

        # Derive realistic order columns from the value counter
        .withColumn("order_id",    col("value"))

        # customer_id cycles through 0-19 (20 unique customers)
        .withColumn("customer_id", (col("value") % 20).cast("int"))

        # amount between 1.0 and 500.0
        .withColumn("amount",      ((col("value") % 500) + 1).cast("double"))

        # region cycles through 4 regions
        .withColumn("region",
            expr("""
                CASE WHEN value % 4 = 0 THEN 'APAC'
                     WHEN value % 4 = 1 THEN 'EMEA'
                     WHEN value % 4 = 2 THEN 'AMER'
                     ELSE 'LATAM'
                END
            """)
        )

        # Every 10th order is CANCELLED — used to test Silver filter rule
        .withColumn("status",
            expr("CASE WHEN value % 10 = 0 THEN 'CANCELLED' ELSE 'COMPLETED' END")
        )

        # Use the built-in rate timestamp as the order event time
        .withColumnRenamed("timestamp", "order_time")

        # Metadata: when did this record land in our pipeline?
        .withColumn("bronze_ingested_at", current_timestamp())

        # Drop the raw value counter — not meaningful to downstream layers
        .drop("value")
    )


# =============================================================================
# SILVER LAYER
# =============================================================================
# Rules for Silver:
#   - Read FROM Bronze (never directly from source)
#   - Apply data quality expectations — this is the "quality gate"
#   - Apply business filtering rules
#   - Deduplicate if needed
#   - Add processed_at timestamp
#
# EXPECTATIONS — Three types, know all three cold for interviews:
#
#   @dlt.expect("name", "condition")
#     → Flags rows that fail but KEEPS them in the table
#     → Use when you want to monitor quality without losing data
#     → Violation count visible in pipeline UI
#
#   @dlt.expect_or_drop("name", "condition")
#     → Drops rows that fail the condition
#     → Use when bad rows would corrupt downstream aggregations
#     → Dropped rows are counted and visible in pipeline event log
#
#   @dlt.expect_or_fail("name", "condition")
#     → Fails the entire pipeline if ANY row violates
#     → Use for critical constraints (e.g. primary key cannot be null)
#     → Like an assertion — zero tolerance
#
# Interview point: "Which expectation type would you use for a financial pipeline?"
# Answer: expect_or_fail for non-nullable keys (order_id, customer_id),
#         expect_or_drop for business rules (positive amounts),
#         expect for soft monitoring (flag unusual regions for review)
# =============================================================================

@dlt.expect_or_fail("order_id must not be null", "order_id IS NOT NULL")
# ↑ If order_id is null, pipeline STOPS — a primary key violation is unacceptable

@dlt.expect_or_drop("amount must be positive", "amount > 0")
# ↑ Drop any row where amount is zero or negative — would corrupt revenue metrics

@dlt.expect("region is known", "region IN ('APAC', 'EMEA', 'AMER', 'LATAM')")
# ↑ Flag unexpected regions but keep the row — soft quality check, monitor over time

@dlt.table(
    name="silver_orders",
    comment="""
        CLEANED layer — validated and filtered orders.
        Cancelled orders removed. Data quality enforced via DLT expectations.
        Quality violations visible in the Pipeline UI under each table node.
    """,
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_orders():
    """
    Reads from bronze_orders using dlt.read_stream().

    dlt.read_stream() vs dlt.read():
      - dlt.read_stream() : incremental streaming read — processes only new rows
      - dlt.read()        : full batch read — re-reads everything each run
      Use read_stream() for Bronze→Silver (append-only source)
      Use read() for Silver→Gold (when you need full aggregation)
    """
    return (
        dlt.read_stream("bronze_orders")        # incremental read from Bronze

        # Business rule: exclude cancelled orders from Silver onwards
        # Cancelled orders remain in Bronze for audit/recovery purposes
        .filter(col("status") == "COMPLETED")

        # Tag when this record was processed through Silver
        .withColumn("silver_processed_at", current_timestamp())

        # Drop status column — all remaining rows are COMPLETED, column is redundant
        .drop("status")
    )


# =============================================================================
# GOLD LAYER
# =============================================================================
# Rules for Gold:
#   - Aggregated business metrics — ready for BI tools, dashboards, ML features
#   - Use dlt.read() not dlt.read_stream() because aggregations are stateful
#   - Recalculated on each pipeline run (full recompute of aggregates)
#   - Column names should be business-friendly (no snake_case abbreviations)
#
# Interview point: "Why use dlt.read() at Gold instead of dlt.read_stream()?"
# Answer: Aggregations like SUM and COUNT need to see ALL rows to be correct.
#         With streaming read, you'd only aggregate the NEW batch — you'd get
#         partial sums per run, not the total. dlt.read() gives you the full
#         Silver table for each pipeline trigger.
# =============================================================================

@dlt.table(
    name="gold_customer_revenue",
    comment="""
        AGGREGATED layer — customer-level revenue summary.
        Rebuilt fully on each pipeline run from Silver.
        Designed for BI dashboards and executive reporting.
        In production: Power BI or Tableau connects here via Databricks SQL.
    """,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_customer_revenue():
    """
    Aggregates Silver orders into per-customer revenue metrics.
    Uses dlt.read() (batch) to get the full Silver dataset each run.
    """
    return (
        dlt.read("silver_orders")               # full batch read — see note above

        .groupBy("customer_id", "region")       # group by customer and region

        .agg(
            _sum("amount").alias("total_revenue"),    # total spend per customer
            count("order_id").alias("total_orders"),  # number of completed orders

            # Average order value — useful business metric
            (_sum("amount") / count("order_id")).alias("avg_order_value")
        )

        # Flag high-value customers — useful for marketing segmentation
        .withColumn("customer_tier",
            expr("""
                CASE WHEN total_revenue > 5000 THEN 'PLATINUM'
                     WHEN total_revenue > 2000 THEN 'GOLD'
                     WHEN total_revenue > 500  THEN 'SILVER'
                     ELSE 'STANDARD'
                END
            """)
        )

        .withColumn("gold_computed_at", current_timestamp())
    )


# COMMAND ----------

# =============================================================================
# WHAT TO OBSERVE IN THE PIPELINE UI (mention this in interviews)
# =============================================================================
# After clicking Start on the Pipeline, you will see:
#
#  1. DAG GRAPH
#     bronze_orders → silver_orders → gold_customer_revenue
#     Each node shows: rows processed, bytes written, expectation violations
#
#  2. EXPECTATION METRICS (on silver_orders node)
#     - "amount must be positive"  : dropped row count
#     - "region is known"          : flagged row count (rows kept)
#     - "order_id must not be null": if violated, pipeline stops
#
#  3. EVENT LOG
#     DLT writes pipeline events to a Delta table automatically.
#     You can query it with:
#       SELECT * FROM event_log('<pipeline_id>')
#       WHERE event_type = 'flow_progress'
#     In production this is your primary observability tool.
#
#  4. DATA QUALITY TAB
#     Visual summary of all expectations across all runs.
#     Interviewers love asking: "How do you monitor DLT pipeline quality?"
#     Answer: "DLT expectations expose metrics in the UI and event log.
#     I'd build a dashboard on top of the event log Delta table using
#     Databricks SQL to track violation trends over time."
# =============================================================================

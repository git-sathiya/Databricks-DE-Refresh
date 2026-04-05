# Databricks notebook source
# DBTITLE 1,Cell 1
# Databricks notebook source
# =============================================================================
# NOTEBOOK   : 02_cdc_apply_changes
# FOLDER     : day1_dlt_lakeflow
# REPO       : Databricks-DE-Refresh
# PURPOSE    : Demonstrates CDC (Change Data Capture) processing using
#              DLT's apply_changes() API - the production pattern for
#              handling INSERT, UPDATE, DELETE events from source systems.
#
# WHAT IS CDC?
#   Change Data Capture is the pattern of capturing every change (insert,
#   update, delete) from a source system (e.g. PostgreSQL via Debezium,
#   or Salesforce via Fivetran) and applying those changes to a target table.
#
# WHY apply_changes() MATTERS IN INTERVIEWS:
#   Before DLT, CDC required complex MERGE logic with deduplication,
#   out-of-order handling, and delete management - all manually coded.
#   apply_changes() handles all of this declaratively in 10 lines.
#
# SCD TYPE 1 vs SCD TYPE 2:
#   SCD Type 1 (default): Keep only the LATEST version of each record
#                         Old values are overwritten. No history retained.
#                         Use for: current state tables (customer address)
#
#   SCD Type 2:           Keep FULL HISTORY of every change
#                         DLT adds __START_AT and __END_AT columns
#                         Use for: audit trails, historical reporting
#
# COMMUNITY EDITION NOTES:
#   - Uses rate() to simulate CDC events (INSERT/UPDATE/DELETE)
#   - No Debezium or Kafka needed
#   - Managed storage via pipeline config
#
# PRODUCTION EQUIVALENT:
#   - Source: Auto Loader reading Debezium JSON from ADLS/S3
#   - Or: Lakeflow Connect (Zerobus) for native CDC ingestion
#   - Target: Unity Catalog managed Delta table
#
# HOW TO RUN:
#   Create a SEPARATE pipeline for this notebook.
#   Pipeline name: cdc_demo_pipeline
#   Source: this notebook
#   Target schema: cdc_demo
#   Mode: Triggered
# =============================================================================

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr, current_timestamp

# =============================================================================
# STEP 1 - SOURCE TABLE (simulates raw CDC events arriving from a source system)
# =============================================================================
# In production this would be Auto Loader reading Debezium-formatted JSON:
# {
#   "customer_id": 3,
#   "name": "Alice",
#   "city": "London",
#   "op": "u",              <- operation: i=insert, u=update, d=delete
#   "ts_ms": 1712345678000  <- source timestamp for ordering
# }
# =============================================================================

@dlt.table(
    name="raw_cdc_events",
    comment="""
        Raw CDC event stream - simulates change events from a source database.
        Each row represents one INSERT, UPDATE, or DELETE operation.
        In production: Auto Loader reading Debezium JSON from cloud storage,
        or Lakeflow Connect pulling directly from PostgreSQL/MySQL.
    """
)
def raw_cdc_events():
    """
    Generates synthetic CDC events using rate() source.

    Every row has:
      customer_id  : the entity being changed (0-4, so only 5 unique customers)
      name         : customer name
      city         : customer city (changes on UPDATE events)
      operation    : INSERT / UPDATE / DELETE
      sequence_num : monotonically increasing - critical for ordering events

    Why sequence_num matters:
      CDC events can arrive OUT OF ORDER (network delays, retries).
      apply_changes() uses sequence_by to resolve which event is "latest"
      by picking the row with the HIGHEST sequence_num for each key.
      Without this, an old UPDATE could overwrite a newer DELETE.
    """
    return (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 3)
        .load()

        # Only 5 unique customers - forces frequent updates to same records
        .withColumn("customer_id",  (col("value") % 5).cast("int"))

        .withColumn("name",
            expr("concat('Customer_', cast(customer_id as string))")
        )

        # City changes based on value - simulates customer address updates
        .withColumn("city",
            expr("""
                CASE WHEN value % 3 = 0 THEN 'London'
                     WHEN value % 3 = 1 THEN 'New York'
                     ELSE 'Singapore'
                END
            """)
        )

        # Operation type: mostly INSERTs and UPDATEs, occasional DELETEs
        # Every 7th event is a DELETE, every 5th is an UPDATE, rest are INSERT
        .withColumn("operation",
            expr("""
                CASE WHEN value % 7 = 0 THEN 'DELETE'
                     WHEN value % 5 = 0 THEN 'UPDATE'
                     ELSE 'INSERT'
                END
            """)
        )

        # sequence_num = value (monotonically increasing)
        # This is what apply_changes uses to resolve ordering
        .withColumn("sequence_num", col("value").cast("long"))

        .withColumn("event_time",   current_timestamp())
        .drop("value", "timestamp")
    )


# =============================================================================
# STEP 2 - DECLARE THE TARGET TABLE
# =============================================================================
# apply_changes() requires you to declare the target table separately
# using dlt.create_streaming_table() before calling apply_changes().
#
# This is different from @dlt.table - you are NOT defining the logic here,
# just registering the schema/table that apply_changes() will write into.
# =============================================================================

dlt.create_streaming_table(
    name="silver_customers_scd1",
    comment="""
        SCD Type 1 target table - always reflects the LATEST state of each customer.
        apply_changes() handles: deduplication, out-of-order events, deletes.
        Only 5 rows max (one per customer_id 0-4) because SCD1 overwrites.

        Interview point: contrast with SCD Type 2 below which keeps full history.
    """
)

# =============================================================================
# STEP 3 - apply_changes() for SCD TYPE 1
# =============================================================================
# This single function call replaces what would be 50+ lines of manual MERGE
# logic with deduplication, null handling, and delete management.
#
# Parameters explained:
#   target        : the streaming table declared above
#   source        : the raw CDC events table
#   keys          : list of columns that uniquely identify a record
#   sequence_by   : column used to determine which event is "most recent"
#                   apply_changes picks the row with MAX(sequence_num) per key
#   apply_as_deletes : boolean expression - when True, the row is a DELETE
#   except_column_list : columns to EXCLUDE from the target table
#                        (operation and sequence_num are CDC metadata, not payload)
# =============================================================================

dlt.apply_changes(
    target          = "silver_customers_scd1",
    source          = "raw_cdc_events",
    keys            = ["customer_id"],          # primary key - must be unique in target
    sequence_by     = "sequence_num",           # resolve out-of-order events
    apply_as_deletes= expr("operation = 'DELETE'"),  # rows with DELETE op are removed
    except_column_list = ["operation", "sequence_num"]  # don't store CDC metadata cols
    # Default: stored_as_scd_type = 1 (overwrite - latest value wins)
)


# COMMAND ----------
# =============================================================================
# STEP 4 - SCD TYPE 2 VARIANT (declare separately, same source)
# =============================================================================
# SCD Type 2 keeps the FULL HISTORY of every change.
# DLT automatically adds two columns to the target:
#   __START_AT : sequence_num when this version of the record became active
#   __END_AT   : sequence_num when this version was superseded (NULL = current)
#
# Example of what the SCD2 table looks like after a few changes:
#
# customer_id | name       | city      | __START_AT | __END_AT
# 0           | Customer_0 | London    | 1          | 5         <- old version
# 0           | Customer_0 | New York  | 5          | 12        <- updated
# 0           | Customer_0 | Singapore | 12         | NULL      <- current
#
# Interview point: "When would you choose SCD2 over SCD1?"
# Answer: SCD2 when you need to answer questions like:
#   "What was this customer's city on a specific date?"
#   "How many times did customers change region last quarter?"
#   Compliance/audit use cases always need SCD2.
# =============================================================================

dlt.create_streaming_table(
    name="silver_customers_scd2",
    comment="""
        SCD Type 2 target - full history of all customer changes.
        DLT adds __START_AT and __END_AT columns automatically.
        Query WHERE __END_AT IS NULL to get current state.
        Query with a specific sequence_num to get point-in-time state.
    """
)

dlt.apply_changes(
    target          = "silver_customers_scd2",
    source          = "raw_cdc_events",
    keys            = ["customer_id"],
    sequence_by     = "sequence_num",
    apply_as_deletes= expr("operation = 'DELETE'"),
    except_column_list = ["operation", "sequence_num"],
    stored_as_scd_type = 2                      # <- only change from SCD1 version
    # DLT now tracks full history with __START_AT and __END_AT columns
)


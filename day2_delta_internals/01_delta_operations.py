# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Databricks notebook source
# ============================================================================
# NOTEBOOK   : 01_delta_operations
# FOLDER     : day2_delta_internals
# REPO       : Databricks-DE-Refresh
# ENVIRONMENT: Databricks Free Edition — Serverless Compute (versionless)
#
# PURPOSE:
#   Demonstrates the core Delta Lake maintenance operations that every
#   Data Engineer must know cold for interviews.
#   Covers the full lifecycle: create → write → optimise → vacuum → inspect.
#
# CORE CONCEPTS:
#   1. Small File Problem    — why it happens and why it hurts query performance
#   2. OPTIMIZE              — compacts small files into larger, efficient files
#   3. ZORDER BY             — co-locates related data within files for fast lookups
#   4. VACUUM                — removes old files no longer referenced by Delta log
#   5. DESCRIBE HISTORY      — the Delta transaction log exposed as a SQL table
#   6. Liquid Clustering     — modern replacement for static partitioning (bonus)
#
# INTERVIEW SCENARIO THIS COVERS:
#   "Your Delta table has 50,000 small files after 3 months of streaming writes.
#    Queries are taking 10x longer than expected. How do you fix it?"
#   Answer: OPTIMIZE to compact + ZORDER on query columns + schedule VACUUM.
#
# HOW TO RUN:
#   Run cells one at a time. Read the comment above each cell before running it.
#   All storage uses Unity Catalog managed tables — no external paths needed.
#=============================================================================

# COMMAND ----------

# COMMAND ----------
# -----------------------------------------------------------------------------
# CELL 1 — Environment setup
# -----------------------------------------------------------------------------
# We use a unique schema name so this notebook does not conflict with
# any existing tables from Day 1 work.
# Unity Catalog path: catalog.schema.table
# In Free Edition: catalog = your workspace default catalog
# -----------------------------------------------------------------------------

# Create a dedicated schema (database) for Day 2 Delta operations
# Use default catalog instead of assuming 'main' exists
spark.sql("CREATE SCHEMA IF NOT EXISTS day2_delta_ops")
print(f"Schema ready: day2_delta_ops")

# Confirm we can see it
spark.sql("SHOW SCHEMAS").filter("databaseName = 'day2_delta_ops'").show()

# COMMAND ----------

# COMMAND
# -----------------------------------------------------------------------------
# CELL 2 — Understand the Small File Problem first (theory into practice)
# -----------------------------------------------------------------------------
# WHY SMALL FILES HURT:
#   Every time a streaming job or a small batch writes to Delta, it creates
#   one or more Parquet files. After weeks of hourly writes, you can end up
#   with thousands of tiny files (e.g. 1MB each instead of 128MB optimal size).
#
#   Impact on reads:
#     - Each file = one separate S3/ADLS API call (LIST + GET)
#     - Spark opens a task per file — 50,000 tasks = massive scheduling overhead
#     - File metadata scanning alone can take minutes
#
# WE SIMULATE THIS by writing 20 separate small batches to the same table.
# Each write() call creates its own set of small Parquet files.
# After this cell, the table will have many small files — just like a real
# streaming table after weeks of micro-batch writes.
# -----------------------------------------------------------------------------

from pyspark.sql.functions import col, current_timestamp, expr, rand, lit
from delta.tables import DeltaTable

# Drop and recreate clean for demo
spark.sql("DROP TABLE IF EXISTS workspace.day2_delta_ops.sales_raw")

print("Writing 20 small batches to simulate the small file problem...")

for batch_num in range(20):
    # Each batch = ~500 rows representing sales transactions
    df = (
        spark.range(500)  # generates 500 rows with id 0..499
        .withColumn("batch_id",    lit(batch_num))
        .withColumn("sale_id",     (col("id") + (batch_num * 500)).cast("long"))
        .withColumn("customer_id", (col("id") % 100).cast("int"))   # 100 unique customers
        .withColumn("product_id",  (col("id") % 50).cast("int"))    # 50 unique products
        .withColumn("region",
            expr("""CASE WHEN id % 4 = 0 THEN 'APAC'
                         WHEN id % 4 = 1 THEN 'EMEA'
                         WHEN id % 4 = 2 THEN 'AMER'
                         ELSE 'LATAM' END"""))
        .withColumn("amount",       (rand() * 1000 + 1).cast("double"))
        .withColumn("sale_date",    current_timestamp())
        .drop("id")
    )

    # append mode = each write creates NEW files (does not overwrite)
    # This is exactly what streaming jobs do — every micro-batch = new files
    df.write.format("delta").mode("append").saveAsTable("workspace.day2_delta_ops.sales_raw")

print("Done. 20 batches written = many small files created.")

# COMMAND ----------

# COMMAND 
#---------------------------------------------------------------------------
# CELL 3 — Inspect file count BEFORE optimisation
# -----------------------------------------------------------------------------
# Delta exposes table file details via DESCRIBE DETAIL.
# The numFiles field shows how many physical Parquet files the table has.
# After 20 batch writes, this should show 20+ small files.
#
# Interview point: "How do you know if a Delta table has a small file problem?"
# Answer: DESCRIBE DETAIL — check numFiles vs sizeInBytes to calculate avg size.
#         If avg file size << 128MB, you have a small file problem.
# -----------------------------------------------------------------------------

detail = spark.sql("DESCRIBE DETAIL workspace.day2_delta_ops.sales_raw")
detail.select("numFiles", "sizeInBytes", "location").show(truncate=False)

# Calculate average file size in KB for context
detail_row = detail.collect()[0]
num_files   = detail_row["numFiles"]
size_bytes  = detail_row["sizeInBytes"]

if num_files > 0:
    avg_kb = (size_bytes / num_files) / 1024
    print(f"\nFiles: {num_files} | Total size: {size_bytes/1024:.1f} KB")
    print(f"Average file size: {avg_kb:.1f} KB")
    print(f"Optimal target: ~128,000 KB (128 MB) per file")
    print(f"Small file problem: {'YES — files are tiny' if avg_kb < 10240 else 'Not severe'}")


# COMMAND ----------

# COMMAND 
# -----------------------------------------------------------------------------
# CELL 4 — OPTIMIZE: Compact small files into larger ones
# -----------------------------------------------------------------------------
# OPTIMIZE reads all the small Parquet files and rewrites them into fewer,
# larger files. The old small files are kept in storage (S3/ADLS) but are
# removed from the Delta transaction log — they become "orphaned" files
# that VACUUM will clean up later.
#
# What OPTIMIZE does NOT do:
#   - Does NOT delete old files immediately (VACUUM does that)
#   - Does NOT change your data — purely a physical file reorganisation
#   - Does NOT block concurrent reads or writes (Delta ACID ensures this)
#
# When to run OPTIMIZE in production:
#   - On a schedule (e.g. daily at midnight) after streaming writes
#   - After large MERGE operations that produce many small files
#   - When query performance degrades significantly
#
# Interview point: "What is the difference between OPTIMIZE and VACUUM?"
# Answer: OPTIMIZE compacts files — a logical operation on the transaction log.
#         VACUUM physically deletes old unreferenced files from storage.
#         You need BOTH: OPTIMIZE first, then VACUUM after the retention period.
# -----------------------------------------------------------------------------

print("Running OPTIMIZE — this compacts small files into larger ones...")
print("In production this would typically run on a daily schedule.\n")

optimize_result = spark.sql("OPTIMIZE workspace.day2_delta_ops.sales_raw")
optimize_result.show(truncate=False)

# Check file count AFTER OPTIMIZE
detail_after = spark.sql("DESCRIBE DETAIL workspace.day2_delta_ops.sales_raw")
detail_after.select("numFiles", "sizeInBytes").show()

print("\nKey observation: numFiles should be significantly reduced after OPTIMIZE.")
print("The old small files still exist on disk — VACUUM will remove them.")


# COMMAND ----------

# COMMAND 
# -----------------------------------------------------------------------------
# CELL 5 — ZORDER BY: Co-locate data for faster queries
# -----------------------------------------------------------------------------
# Z-Ordering is a multi-dimensional data clustering technique.
# It reorders rows WITHIN each Parquet file so that rows with similar
# values for the Z-order columns are stored physically close together.
#
# HOW IT HELPS:
#   When you query "WHERE region = 'APAC' AND customer_id = 42",
#   Delta's data skipping reads the file-level min/max statistics.
#   With Z-ordering, APAC rows are clustered together → entire files
#   can be skipped rather than scanning every file.
#
# OPTIMIZE + ZORDER = compaction AND clustering in one operation.
#
# ZORDER vs PARTITION BY — know this cold for interviews:
#   PARTITION BY:
#     - Physically splits data into separate folders per partition value
#     - Best for: low-cardinality columns (date, region — < 1000 unique values)
#     - Risk: over-partitioning creates MORE small files (e.g. partitioning by id)
#     - Prunes at folder level — very fast for equality filters on that column
#
#   ZORDER BY:
#     - Clusters data WITHIN files using space-filling curves (Z-curve)
#     - Best for: high-cardinality columns used in WHERE/JOIN (customer_id, product_id)
#     - Can Z-order on multiple columns simultaneously
#     - Works WITH partitioning — partition by date, Z-order by customer_id within
#
#   RULE OF THUMB:
#     - Partition by: date, region, status (columns you always filter on, low cardinality)
#     - Z-order by: customer_id, product_id, user_id (columns in ad-hoc WHERE clauses)
# -----------------------------------------------------------------------------

print("Running OPTIMIZE with ZORDER BY region, customer_id...")
print("This clusters data so APAC queries skip non-APAC files entirely.\n")

zorder_result = spark.sql("""
    OPTIMIZE workspace.day2_delta_ops.sales_raw
    ZORDER BY (region, customer_id)
""")
zorder_result.show(truncate=False)

# Prove Z-ordering works — run a filtered query and check metrics
print("\nRunning a filtered query to demonstrate data skipping after ZORDER...")
filtered = spark.sql("""
    SELECT region, COUNT(*) as orders, SUM(amount) as revenue
    FROM workspace.day2_delta_ops.sales_raw
    WHERE region = 'APAC'
    GROUP BY region
""")
filtered.show()

print("""
After ZORDER:
  - Delta's file statistics know which files contain APAC data
  - Files with NO APAC data are completely skipped (never read)
  - Check Spark UI → SQL tab → 'files pruned' metric to confirm skipping
""")


# COMMAND ----------

# COMMAND 
# -----------------------------------------------------------------------------
# CELL 6 — VACUUM: Remove orphaned files to reclaim storage
# -----------------------------------------------------------------------------
# VACUUM deletes Parquet files that are no longer referenced by the Delta
# transaction log. These "orphaned" files are created by:
#   - OPTIMIZE (old small files replaced by new large files)
#   - DELETE operations (old version files)
#   - MERGE operations (old files replaced by merged files)
#
# CRITICAL: Default retention = 7 days (168 hours).
#   VACUUM will NOT delete files newer than the retention threshold.
#   This protects time travel — if you set retention to 7 days,
#   you can time-travel back 7 days but not further.
#
# WARNING about short retention:
#   Setting VACUUM retention < 7 days risks breaking concurrent readers
#   who might be reading an older snapshot of the table.
#   In production NEVER go below 7 days without a specific reason.
#
# For this demo we use 0 hours (deletes everything immediately) ONLY
# because this is a practice table with no concurrent readers.
# In a real interview, say: "I would never use 0 hours in production."
#
# Interview point: "What happens if you VACUUM a Delta table then try to
#                  time travel to a version older than the retention period?"
# Answer: You get a VersionNotFoundException — the Parquet files for that
#         version have been physically deleted. The transaction log entry
#         still exists but the underlying data files are gone.
# -----------------------------------------------------------------------------

# First, check what VACUUM WOULD delete without actually deleting
print("DRY RUN — showing files that would be deleted by VACUUM...")

spark.sql("""
    VACUUM workspace.day2_delta_ops.sales_raw
    RETAIN 168 HOURS   -- 7 days — production-safe default
    DRY RUN            -- preview only, does not delete anything
""").show(truncate=False)

# COMMAND 
# Now actually run VACUUM with 0 hours for demo purposes only
# This is to show a clean state — never do this in production
print("Running VACUUM with 0 hours (demo only — not for production use)...")
print("In production use: VACUUM table_name RETAIN 168 HOURS\n")

# In serverless compute, set table property to allow short retention
spark.sql("""
    ALTER TABLE workspace.day2_delta_ops.sales_raw 
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration'='interval 0 hours')
""")

spark.sql("""
    VACUUM workspace.day2_delta_ops.sales_raw
    RETAIN 0 HOURS
""")

print("VACUUM complete. Old small files from before OPTIMIZE are now deleted.")
print("Time travel to pre-OPTIMIZE versions is no longer possible.")

# COMMAND ----------

# COMMAND 
#---------------------------------------------------------------------------
# CELL 7 — DESCRIBE HISTORY: The Delta transaction log as a SQL table
# ----------------------------------------------------------------------------
# Every operation on a Delta table creates a new entry in the transaction log.
# DESCRIBE HISTORY shows this log in a human-readable format.
#
# This is your PRIMARY debugging and auditing tool in production.
# Every write, OPTIMIZE, VACUUM, MERGE, DELETE, RESTORE shows up here.
#
# Key columns:
#   version          : monotonically increasing version number
#   timestamp        : when the operation happened
#   operation        : WRITE, MERGE, OPTIMIZE, VACUUM, RESTORE etc.
#   operationMetrics : rows added/removed, files added/removed, bytes written
#   userName         : who ran the operation (important for audit)
#
# Interview point: "How do you audit who deleted rows from a production Delta table?"
# Answer: DESCRIBE HISTORY — filter operation = 'DELETE', check userName and
#         operationMetrics to see how many rows were affected and when.
# ----------------------------------------------------------------------------
print("Full transaction history of our sales_raw table:")
print("Every operation we ran is recorded here — this is Delta's audit log.\n")

spark.sql("""
    DESCRIBE HISTORY workspace.day2_delta_ops.sales_raw
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics",
    "userName"
).show(30, truncate=False)

# COMMAND ----------

# COMMAND 
#---------------------------------------------------------------------------
# CELL 8 — LIQUID CLUSTERING (bonus — modern alternative to partitioning)
#---------------------------------------------------------------------------
# Liquid Clustering was introduced in Databricks Runtime 13.3 LTS.
# It is the recommended replacement for static PARTITION BY in new tables.
#
# WHY LIQUID CLUSTERING IS BETTER THAN STATIC PARTITIONS:
#   Problem with PARTITION BY:
#     - You must choose the partition column at CREATE TABLE time
#     - Changing it requires rewriting the entire table
#     - Over-partitioning (e.g. by high-cardinality column) = small file explosion
#
#   Liquid Clustering advantages:
#     - Clustering column(s) can be changed WITHOUT rewriting data
#     - Incremental clustering — only newly written data needs re-clustering
#     - Works automatically with OPTIMIZE
#     - Better for evolving query patterns (you don't know all future queries)
#
# Interview point: "When would you choose Liquid Clustering over PARTITION BY?"
# Answer: Liquid Clustering for new tables where query patterns may evolve,
#         or where cardinality is high. PARTITION BY still valid for well-known,
#         stable, low-cardinality filter columns like date or status.
# ----------------------------------------------------------------------------
# Create a new table WITH liquid clustering enabled at table creation
spark.sql("""
    CREATE OR REPLACE TABLE workspace.day2_delta_ops.sales_clustered
    CLUSTER BY (region, customer_id)  -- liquid clustering defined here
    AS
    SELECT * FROM workspace.day2_delta_ops.sales_raw
""")

print("Table created with Liquid Clustering on (region, customer_id).")
print("OPTIMIZE on this table will automatically use clustering instead of Z-ordering.\n")

# Inspect the table properties to confirm clustering
spark.sql("DESCRIBE DETAIL workspace.day2_delta_ops.sales_clustered") \
     .select("clusteringColumns", "numFiles") \
     .show(truncate=False)

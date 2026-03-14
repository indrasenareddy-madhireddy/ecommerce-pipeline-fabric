# =============================================================================
# PROJECT  : E-Commerce Sales Analytics Pipeline
# LAYER    : Bronze — Raw Ingestion
# PLATFORM : Microsoft Fabric (PySpark)
# AUTHOR   : Indrasenareddy Madhireddy
# PURPOSE  : Read raw CSV files from Lakehouse Files section and write them
#            as Delta tables without any transformations. Bronze is the
#            archive layer — raw data is preserved exactly as it arrives.
# =============================================================================
# TABLES CREATED:
#   - Tables/bronze_orders    (13 rows — includes duplicates and nulls)
#   - Tables/bronze_customers (7 rows)
#   - Tables/bronze_products  (5 rows)
# =============================================================================

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import current_timestamp, lit

# ── Step 1: Verify Environment ────────────────────────────────────────────────
# Always verify Spark is running and raw files are accessible
# before starting any pipeline processing
print("Spark version:", spark.version)

files = mssparkutils.fs.ls("Files/")
for f in files:
    print(f.name, "-", f.size, "bytes")

# ── Step 2: Bronze Orders ─────────────────────────────────────────────────────
# Define schema manually for orders.
# order_date is kept as StringType in Bronze — type conversion happens in Silver.
# This ensures Bronze is a faithful copy of the raw source data.
orders_schema = StructType([
    StructField("order_id",    StringType(),  True),
    StructField("customer_id", StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("order_date",  StringType(),  True),  # kept as string in Bronze
    StructField("status",      StringType(),  True),
    StructField("amount",      DoubleType(),  True)
])

# Read raw CSV with manual schema — no inferSchema in production
df = spark.read \
    .option("header", True) \
    .schema(orders_schema) \
    .csv("Files/orders.csv")

# Add metadata columns — when the data arrived and from which file
# These columns help with auditing and debugging in production
df = df.withColumn("ingestion_timestamp", current_timestamp())
df = df.withColumn("source_file", lit("orders.csv"))

# Write to Delta — overwrite mode ensures idempotent pipeline runs
# Running the pipeline multiple times always produces the same result
df.write.format("delta").mode("overwrite").save("Tables/bronze_orders")

# Verify write was successful by reading back
bronze_orders = spark.read.format("delta").load("Tables/bronze_orders")
print("Bronze Orders rows:", bronze_orders.count())
display(bronze_orders)

# ── Step 3: Bronze Customers ──────────────────────────────────────────────────
# Use inferSchema to automatically detect column types from the CSV.
# This is acceptable in Bronze because we are just archiving raw data.
# The inferred schema is locked into a variable for reuse — avoids
# scanning the file twice which would be wasteful on large datasets.
df_explore = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/customers.csv")

# Lock the inferred schema — use it for the actual production read
customers_schema = df_explore.schema

customers_df = spark.read \
    .option("header", True) \
    .schema(customers_schema) \
    .csv("Files/customers.csv")

# Add metadata columns using withColumns (plural) — adds both columns
# in a single operation which is more efficient than chaining withColumn calls
customers_df = customers_df.withColumns({
    "ingestion_timestamp": current_timestamp(),
    "source_file":         lit("customers.csv")
})

customers_df.write.format("delta").mode("overwrite").save("Tables/bronze_customers")

bronze_customers = spark.read.format("delta").load("Tables/bronze_customers")
print("Bronze Customers rows:", bronze_customers.count())
display(bronze_customers)

# ── Step 4: Bronze Products ───────────────────────────────────────────────────
# Same inferSchema pattern as customers.
# overwriteSchema=true is used here because the schema was changed during
# development (column renamed from sourcefile to source_file).
# In production this option would only be used for intentional schema changes.
df_explore = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/products.csv")

products_schema = df_explore.schema

df = spark.read \
    .option("header", True) \
    .schema(products_schema) \
    .csv("Files/products.csv")

products_df = df.withColumns({
    "ingestion_timestamp": current_timestamp(),
    "source_file":         lit("products.csv")
})

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("Tables/bronze_products")

bronze_products = spark.read.format("delta").load("Tables/bronze_products")
print("Bronze Products rows:", bronze_products.count())
display(bronze_products)

# ── Step 5: Delta Time Travel Verification ────────────────────────────────────
# Delta Lake automatically tracks every write as a versioned transaction.
# This allows us to audit the history of any table and recover previous
# versions if something goes wrong in production.

# View full version history of bronze_products
spark.sql("DESCRIBE HISTORY delta.`Tables/bronze_products`") \
    .select("version", "timestamp", "operation", "operationParameters") \
    .show(truncate=False)

# Read a specific previous version — useful for debugging and audits
# versionAsOf=0 reads the very first write to this table
old_df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("Tables/bronze_products")

print("Version 0 of bronze_products:")
display(old_df)

# ── Bronze Layer Complete ─────────────────────────────────────────────────────
print("=" * 60)
print("BRONZE LAYER COMPLETE")
print("Tables created: bronze_orders, bronze_customers, bronze_products")
print("All raw data archived as Delta tables in OneLake")
print("=" * 60)

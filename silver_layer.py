# =============================================================================
# PROJECT  : E-Commerce Sales Analytics Pipeline
# LAYER    : Silver — Cleaning and Validation
# PLATFORM : Microsoft Fabric (PySpark)
# AUTHOR   : Indrasenareddy Madhireddy
# PURPOSE  : Read from Bronze Delta tables, apply data quality rules,
#            fix data types, standardise formats, and write clean trusted
#            data to Silver Delta tables. Silver is the single source of
#            truth — all Gold tables are built on top of Silver.
# =============================================================================
# TRANSFORMATIONS APPLIED:
#   Orders    — remove duplicates, drop critical nulls, fill remaining nulls,
#               fix data types, add data quality flag
#   Customers — trim whitespace, fix name casing, lowercase emails
#   Products  — pass through clean data, add timestamp
# =============================================================================
# TABLES CREATED:
#   - Tables/silver_orders    (11 rows — 2 removed from Bronze)
#   - Tables/silver_customers (7 rows)
#   - Tables/silver_products  (5 rows)
# =============================================================================

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)
from pyspark.sql.functions import (
    col, trim, initcap, lower,
    to_date, when, lit,
    current_timestamp, count
)

# ── Step 1: Read from Bronze Layer ────────────────────────────────────────────
# Silver always reads from Bronze — never from raw CSV files directly.
# This enforces the Medallion Architecture principle:
# each layer only reads from the layer directly below it.
orders   = spark.read.format("delta").load("Tables/bronze_orders")
customers = spark.read.format("delta").load("Tables/bronze_customers")
products  = spark.read.format("delta").load("Tables/bronze_products")

# Verify all Bronze tables are accessible before starting transformations
for name, df in [("Orders", orders), ("Customers", customers), ("Products", products)]:
    print(f"{name}: {df.count()} rows")
    df.printSchema()

# ── Step 2: Silver Orders ─────────────────────────────────────────────────────
# Apply all data quality rules in a logical order:
# 1. Remove duplicates first — eliminates redundant rows before any processing
# 2. Drop rows with null critical columns — orders without customer or product
#    cannot be processed and must be rejected
# 3. Fill remaining nulls — non-critical nulls get sensible defaults
# 4. Fix data types — convert strings to proper types for accurate calculations
# 5. Add data quality flag — track which rows had issues for analyst review
# 6. Add updated_timestamp — record when this row was last processed

# Step 2a — Remove duplicates and drop critical nulls
df = orders \
    .dropDuplicates(["order_id"]) \
    .dropna(subset=["customer_id", "product_id"]) \
    .fillna({"status": "unknown", "amount": 0.0})

# Step 2b — Fix data types
# order_date: string "2024-01-10" → proper DateType for date calculations
# quantity: ensure integer type for aggregations
# amount: ensure double type for financial calculations
df = df.withColumns({
    "order_date": to_date(col("order_date"), "yyyy-MM-dd"),
    "quantity":   col("quantity").cast(IntegerType()),
    "amount":     col("amount").cast(DoubleType())
})

# Step 2c — Add data quality flag and audit timestamp
# Data quality flag preserves evidence of original data issues
# even after they have been fixed — critical for production auditing
df = df.withColumns({
    "data_quality_flag": when(
        (col("amount") == 0.0) & (col("status") == "unknown"),
        "missing_amount&missing_status"
    )
    .when(col("amount") == 0.0, "missing_amount")
    .when(col("status") == "unknown",  "missing_status")
    .otherwise("clean"),
    "updated_timestamp": current_timestamp()
})

# Write Silver Orders to Delta
df.write.format("delta").mode("overwrite").save("Tables/silver_orders")

silver_orders = spark.read.format("delta").load("Tables/silver_orders")
print("Silver Orders rows:", silver_orders.count())
display(silver_orders)

# ── Step 3: Silver Customers ──────────────────────────────────────────────────
# Fix three data quality issues in customer names:
# 1. Extra whitespace — trim() removes leading and trailing spaces
# 2. Inconsistent casing — initcap() converts to Proper Case
#    (applied after trim so spaces don't affect word detection)
# 3. Email — lower() standardises to lowercase for consistent lookups
# 4. City — initcap() for consistent display in reports

df = spark.read.format("delta").load("Tables/bronze_customers")

df = df.withColumns({
    "customer_name": initcap(trim(col("customer_name"))),  # trim first then initcap
    "email":         lower(col("email")),
    "city":          initcap(col("city"))
})

df = df.withColumn("updated_timestamp", current_timestamp())

df.write.format("delta").mode("overwrite").save("Tables/silver_customers")

silver_customers = spark.read.format("delta").load("Tables/silver_customers")
print("Silver Customers rows:", silver_customers.count())
display(silver_customers)

# ── Step 4: Silver Products ───────────────────────────────────────────────────
# Products data arrived clean — no cleaning required.
# Still written to Silver layer for consistency — Gold always reads from Silver,
# never directly from Bronze, regardless of data quality.

df = spark.read.format("delta").load("Tables/bronze_products")
df = df.withColumn("updated_timestamp", current_timestamp())

df.write.format("delta").mode("overwrite").save("Tables/silver_products")

silver_products = spark.read.format("delta").load("Tables/silver_products")
print("Silver Products rows:", silver_products.count())
display(silver_products)

# ── Silver Layer Complete ─────────────────────────────────────────────────────
print("=" * 60)
print("SILVER LAYER COMPLETE")
print("bronze_orders    13 rows → silver_orders    11 rows (2 removed)")
print("bronze_customers  7 rows → silver_customers  7 rows (names fixed)")
print("bronze_products   5 rows → silver_products   5 rows (pass through)")
print("All Silver tables are clean, typed, and ready for Gold layer")
print("=" * 60)

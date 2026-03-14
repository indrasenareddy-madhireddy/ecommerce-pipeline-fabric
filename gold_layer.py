# =============================================================================
# PROJECT  : E-Commerce Sales Analytics Pipeline
# LAYER    : Gold — Analytics Ready
# PLATFORM : Microsoft Fabric (PySpark)
# AUTHOR   : Indrasenareddy Madhireddy
# PURPOSE  : Read from Silver Delta tables, join datasets, calculate business
#            metrics, and write analytics-ready Gold tables that directly
#            answer business questions. Gold tables are consumed by Power BI
#            dashboards and business reporting tools.
# =============================================================================
# BUSINESS QUESTIONS ANSWERED:
#   1. What does a complete order look like with customer and product details?
#   2. Which cities generate the most revenue?
#   3. Which product categories sell the best?
#   4. Who are our top spending customers?
# =============================================================================
# TABLES CREATED:
#   - Tables/gold_enriched_orders    (8 rows — completed orders only)
#   - Tables/gold_sales_by_city      (4 rows — revenue by city)
#   - Tables/gold_sales_by_category  (4 rows — revenue by category)
#   - Tables/gold_top_customers      (5 rows — ranked by total spend)
# =============================================================================

# ── Imports ──────────────────────────────────────────────────────────────────
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, sum, count, avg, round,
    rank, desc, broadcast,
    current_timestamp, lit, dense_rank
)
from pyspark.sql import Window

# ── Step 1: Read from Silver Layer ────────────────────────────────────────────
# Gold always reads from Silver — never from Bronze or raw files directly.
# Silver data is clean, typed, and validated — safe for business calculations.
sil_orders    = spark.read.format("delta").load("Tables/silver_orders")
sil_customers = spark.read.format("delta").load("Tables/silver_customers")
sil_products  = spark.read.format("delta").load("Tables/silver_products")

# Verify all Silver tables loaded correctly before joining
for name, df in [
    ("sil_orders",    sil_orders),
    ("sil_customers", sil_customers),
    ("sil_products",  sil_products)
]:
    print(f"{name} rows count is: {df.count()}")

# ── Step 2: Gold Table 1 — Enriched Orders ────────────────────────────────────
# Join orders with customers and products to create a denormalised view
# showing full context of every completed transaction.
#
# Performance optimisation — Broadcast Join:
# sil_customers (7 rows) and sil_products (5 rows) are very small tables.
# Wrapping them in broadcast() sends a full copy to every Spark executor
# instead of shuffling the larger orders table across the network.
# This significantly reduces data movement and improves join performance.
#
# Only completed orders are included — cancelled and pending orders
# are excluded from Gold as they do not represent confirmed revenue.

df = sil_orders.filter(col("status") == "completed")

# Join with customers — broadcast small lookup table
df = df.join(broadcast(sil_customers), "customer_id", "left")

# Join with products — broadcast small lookup table
df = df.join(broadcast(sil_products), "product_id", "left")

# Select only business-relevant columns — drop metadata and technical columns
df = df.select(
    "order_id", "order_date", "customer_id", "customer_name",
    "city", "product_id", "product_name", "category",
    "quantity", "amount", "data_quality_flag"
)

df = df.withColumn("updated_timestamp", current_timestamp())

df.write.format("delta").mode("overwrite").save("Tables/gold_enriched_orders")

gold_enriched = spark.read.format("delta").load("Tables/gold_enriched_orders")
print("Gold Enriched Orders rows:", gold_enriched.count())
display(gold_enriched)

# ── Step 3: Gold Table 2 — Sales by City ──────────────────────────────────────
# Aggregate completed order revenue by city.
# Answers: Which cities generate the most revenue?
# Business use: Marketing team uses this to target high-value cities.

df = spark.read.format("delta").load("Tables/gold_enriched_orders")

df = df.groupBy("city").agg(
    count("order_id").alias("total_orders"),
    round(sum("amount"),  2).alias("total_revenue"),
    round(avg("amount"),  2).alias("avg_order_value")
).orderBy(col("total_revenue").desc())

df.write.format("delta").mode("overwrite").save("Tables/gold_sales_by_city")
print("Gold Sales by City:")
display(df)

# ── Step 4: Gold Table 3 — Sales by Category ──────────────────────────────────
# Aggregate completed order revenue by product category.
# Answers: Which product categories drive the most sales?
# Business use: Inventory and procurement teams use this for stock planning.

df = spark.read.format("delta").load("Tables/gold_enriched_orders")

df = df.groupBy("category").agg(
    count("order_id").alias("total_orders"),
    round(sum("amount"),  2).alias("total_revenue"),
    round(avg("amount"),  2).alias("avg_order_value")
).orderBy(col("total_revenue").desc())

df.write.format("delta").mode("overwrite").save("Tables/gold_sales_by_category")
print("Gold Sales by Category:")
display(df)

# ── Step 5: Gold Table 4 — Top Customers ──────────────────────────────────────
# Rank customers by total spending using Window function.
# Answers: Who are our highest value customers?
# Business use: CRM and loyalty teams use this to prioritise top customers.
#
# Window function explanation:
# dense_rank() is used instead of rank() to avoid gaps in rankings
# when two customers have equal spending — ensures clean 1,2,3 numbering.

df = spark.read.format("delta").load("Tables/gold_enriched_orders")

df = df.groupBy("customer_id", "customer_name").agg(
    count("order_id").alias("total_orders"),
    round(sum("amount"), 2).alias("total_spent")
)

# Define window — partition not needed here as we rank all customers together
window_spec = Window.orderBy(col("total_spent").desc())

# Add rank column — dense_rank avoids gaps when customers have equal spending
df = df.withColumn("rank", dense_rank().over(window_spec))
df = df.orderBy("rank")

df.write.format("delta").mode("overwrite").save("Tables/gold_top_customers")
print("Gold Top Customers:")
display(df)

# ── Gold Layer Complete ───────────────────────────────────────────────────────
print("=" * 60)
print("GOLD LAYER COMPLETE")
print("gold_enriched_orders   — 8 rows  — full order details")
print("gold_sales_by_city     — 4 rows  — revenue by city")
print("gold_sales_by_category — 4 rows  — revenue by category")
print("gold_top_customers     — 5 rows  — ranked by spend")
print("All Gold tables ready for Power BI and business reporting")
print("=" * 60)

# E-Commerce Sales Analytics Pipeline

End-to-end batch data pipeline built on **Microsoft Fabric** using **PySpark**, **Delta Lake**, and **Medallion Architecture** (Bronze → Silver → Gold).

---

## Project Overview

ShopEasy is a growing e-commerce company selling electronics, fashion, and appliances across India. Raw transactional data arrives daily as CSV files but is messy — duplicates, nulls, wrong formats, and inconsistent casing make it unusable for business reporting.

This pipeline transforms raw messy data into clean, reliable, analytics-ready Delta tables that directly answer business questions.

---

## Architecture

```
Raw CSV Files (Orders, Customers, Products)
        ↓
┌─────────────────────────────────────────┐
│  BRONZE LAYER — Raw Ingestion           │
│  • Read CSV files with schema           │
│  • Add ingestion_timestamp metadata     │
│  • Write to Delta tables (archive)      │
└─────────────────────────────────────────┘
        ↓
┌─────────────────────────────────────────┐
│  SILVER LAYER — Cleaning & Validation   │
│  • Remove duplicates                    │
│  • Handle nulls (drop critical, fill)   │
│  • Fix data types (string → date/int)   │
│  • Standardise formats (trim, initcap)  │
│  • Add data_quality_flag column         │
└─────────────────────────────────────────┘
        ↓
┌─────────────────────────────────────────┐
│  GOLD LAYER — Analytics Ready           │
│  • Join orders + customers + products   │
│  • Aggregate by city, category          │
│  • Rank top customers (window function) │
│  • Ready for Power BI consumption       │
└─────────────────────────────────────────┘
```

---

## Tech Stack

| Technology | Usage |
|------------|-------|
| Microsoft Fabric | Platform — Lakehouse, Notebooks, OneLake |
| PySpark | Data transformation and processing |
| Delta Lake | Storage format — ACID, time travel, versioning |
| Python | Pipeline scripting |

---

## Pipeline Notebooks

| Notebook | Purpose |
|----------|---------|
| `bronze_layer.py` | Raw CSV ingestion → Delta tables |
| `silver_layer.py` | Data cleaning and validation |
| `gold_layer.py` | Business metrics and aggregations |

---

## Delta Tables Created

| Table | Layer | Rows | Description |
|-------|-------|------|-------------|
| bronze_orders | Bronze | 13 | Raw orders including duplicates and nulls |
| bronze_customers | Bronze | 7 | Raw customer data |
| bronze_products | Bronze | 5 | Raw product catalog |
| silver_orders | Silver | 11 | Cleaned orders — 2 rows removed |
| silver_customers | Silver | 7 | Names and emails standardised |
| silver_products | Silver | 5 | Pass-through clean data |
| gold_enriched_orders | Gold | 8 | Completed orders with full details |
| gold_sales_by_city | Gold | 4 | Revenue aggregated by city |
| gold_sales_by_category | Gold | 4 | Revenue aggregated by category |
| gold_top_customers | Gold | 5 | Customers ranked by total spend |

---

## Key Engineering Concepts Applied

**Schema Management** — Manual StructType schema definition for orders. inferSchema with schema locking for customers and products.

**Data Quality Flagging** — Rows with missing amount or status are flagged with a data_quality_flag column before being fixed — preserving evidence of original issues.

**Broadcast Joins** — sil_customers (7 rows) and sil_products (5 rows) are broadcast to all Spark executors to avoid expensive shuffles when joining with orders.

**Window Functions** — dense_rank() over a total_spent window to rank customers without gaps in ranking when values are equal.

**Delta Time Travel** — Full version history maintained on all Delta tables. Previous versions can be read using versionAsOf or timestampAsOf for auditing and recovery.

**Idempotent Pipeline** — All writes use overwrite mode — running the pipeline multiple times always produces the same clean result.

---

## Business Insights Produced

- **Mumbai** generates the highest total revenue (₹2,850) across 3 orders
- **Electronics** is the top performing category (₹5,600 revenue, 4 orders)
- **Alice Smith** is the top customer with ₹2,850 total spend
- **Delhi** has the highest average order value (₹1,000) despite fewer orders

---

## Data Quality Issues Handled

| Issue | Location | Resolution |
|-------|----------|------------|
| Duplicate order (order_id 1001) | bronze_orders | Removed in Silver |
| Missing customer_id (order 1011) | bronze_orders | Row dropped in Silver |
| Missing status (order 1012) | bronze_orders | Filled with "unknown" |
| Missing amount (order 1010) | bronze_orders | Filled with 0.0, flagged |
| Inconsistent name casing (BOB JONES) | bronze_customers | Fixed with initcap() |
| Extra whitespace in names | bronze_customers | Fixed with trim() |

---

## Author

**Indrasenareddy Madhireddy**
Data Engineer — PySpark · Delta Lake · Microsoft Fabric · Azure

# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import time
import uuid

# COMMAND ----------

dbutils.widgets.text("catalog", "huntington_ingalls_industries_catalog")
catalog = dbutils.widgets.get("catalog")
dbutils.widgets.text("schema", "smart_stock")
schema = dbutils.widgets.get("schema")
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

df_silver_products = spark.table(f"{catalog}.{schema}.silver_products")
df_silver_product_plants = spark.table(f"{catalog}.{schema}.silver_product_plants")
df_silver_warehouses = spark.table(f"{catalog}.{schema}.silver_warehouses")
df_silver_transactions = spark.table(f"{catalog}.{schema}.silver_inventory_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dim Products

# COMMAND ----------

# Filter current records only (SCD Type 2 → Type 1)
df_products_current = df_silver_products.filter(F.col("is_current") == True)

print(f"   📊 Current products: {df_products_current.count():,} (filtered from {df_silver_products.count():,})")

# COMMAND ----------

df_reorder = df_silver_product_plants.groupBy("material_number").agg(
    F.avg("reorder_level").alias("avg_reorder_level")
)

# Join with products
df_products_with_reorder = df_products_current.join(
    df_reorder,
    on="material_number",
    how="left"
)

# Fill nulls with default reorder level
df_products_with_reorder = df_products_with_reorder.withColumn(
    "reorder_level",
    F.coalesce(F.col("avg_reorder_level"), F.lit(20)).cast("int")
)

# Sort by material_number for consistent ID assignment
df_products_sorted = df_products_with_reorder.orderBy("material_number")

# COMMAND ----------

# Create sequential product IDs using row_number
window_spec = Window.orderBy("material_number")

df_gold_products = df_products_sorted.withColumn(
    "product_id",
    F.row_number().over(window_spec)
).select(
    F.col("product_id"),
    F.col("product_name").alias("name"),
    F.col("product_description").alias("description"),
    F.col("sku"),
    F.col("price"),
    F.col("unit_of_measure").alias("unit"),
    F.col("category"),
    F.col("reorder_level"),
    F.col("created_at"),
    F.col("updated_at"),
    # Keep for joins (will drop later)
    F.col("material_number").alias("_material_number"),
    F.col("product_key").alias("_product_key")
)

# COMMAND ----------

display(df_gold_products)

# COMMAND ----------

df_gold_products.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .saveAsTable(f"{catalog}.{schema}.dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dim Warehouse

# COMMAND ----------

df_warehouses_current = df_silver_warehouses.filter(F.col("is_current") == True)

print(f"   📊 Current warehouses: {df_warehouses_current.count():,}")

# COMMAND ----------

# Sort by plant_code for consistent ID assignment (FR01=1, DE01=2, IT01=3)
df_warehouses_sorted = df_warehouses_current.orderBy("plant_code")

# Assign sequential IDs
window_spec = Window.orderBy("plant_code")

df_gold_warehouses = df_warehouses_sorted.withColumn(
    "warehouse_id",
    F.row_number().over(window_spec)
).select(
    F.col("warehouse_id"),
    F.col("warehouse_name").alias("name"),
    F.col("location_address").alias("location"),
    F.col("manager_id"),
    F.col("timezone"),
    F.col("created_at"),
    F.col("updated_at"),
    # Keep for joins (will drop later)
    F.col("plant_code").alias("_plant_code"),
    F.col("warehouse_key").alias("_warehouse_key")
)

# COMMAND ----------

display(df_gold_warehouses)

# COMMAND ----------

df_gold_warehouses.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.dim_warehouses")

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact Inventory Transactions

# COMMAND ----------

df_products_map = df_gold_products.select("product_id", "_product_key")

df_warehouses_map =df_gold_warehouses.select("warehouse_id", "_warehouse_key")

# COMMAND ----------

display(df_products_map)

# COMMAND ----------

# Join transactions with dimension mappings
df_transactions_with_ids = df_silver_transactions \
    .join(df_products_map, df_silver_transactions.product_key == df_products_map._product_key, "inner") \
    .join(df_warehouses_map, df_silver_transactions.warehouse_key == df_warehouses_map._warehouse_key, "inner")

# COMMAND ----------

df_transactions_sorted = df_transactions_with_ids.orderBy("transaction_timestamp")

# Assign sequential transaction IDs
window_spec = Window.orderBy("transaction_timestamp")

df_gold_transactions = df_transactions_sorted.withColumn(
    "transaction_id",
    F.row_number().over(window_spec)
).select(
    F.col("transaction_id"),
    F.col("transaction_number"),
    F.col("product_id"),
    F.col("warehouse_id"),
    F.col("quantity_change"),
    F.col("transaction_type"),
    F.col("status"),
    F.col("notes"),
    F.col("transaction_timestamp"),
    F.col("updated_at")
)

# COMMAND ----------

display(df_gold_transactions)

# COMMAND ----------

df_gold_transactions.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.fact_inventory_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC # Daily Inventory Snapshots

# COMMAND ----------

# Get date range from transactions
date_stats = df_gold_transactions.select(
    F.min(F.to_date("transaction_timestamp")).alias("min_date"),
    F.max(F.to_date("transaction_timestamp")).alias("max_date")
).collect()[0]

min_date = date_stats['min_date']
max_date = date_stats['max_date']

print(f"   📅 Date range: {min_date} to {max_date}")
print(f"   📊 Total days: {(max_date - min_date).days + 1}")

# COMMAND ----------

# Get all product-warehouse combinations
product_warehouse_combos = df_gold_transactions.select(
    "product_id",
    "warehouse_id"
).distinct()

print(f"   🔢 Product-Warehouse combinations: {product_warehouse_combos.count():,}")

# COMMAND ----------

# Load product reorder levels for initial inventory calculation
df_products_reorder = df_gold_products \
    .select("product_id", "reorder_level")

# Create initial inventory (2-4x reorder level) for each product-warehouse combo
from pyspark.sql.functions import rand

df_initial_inventory = product_warehouse_combos.join(
    df_products_reorder,
    on="product_id",
    how="inner"
).withColumn(
    "initial_inventory",
    (F.col("reorder_level") * (4.0 + rand() * 4.0)).cast("int")
).select(
    "product_id",
    "warehouse_id",
    "initial_inventory"
)

print(f"   ✅ Initialized starting inventory for {df_initial_inventory.count():,} combinations")

# COMMAND ----------

# Create a date spine (all dates between min and max)
from pyspark.sql.functions import explode, sequence, to_date

df_dates = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    )) as snapshot_date
""")

print(f"   📅 Generated date spine: {df_dates.count():,} dates")

# COMMAND ----------

# Create product-warehouse-date combinations
df_date_combos = product_warehouse_combos.crossJoin(df_dates)

print(f"   🔢 Total snapshot combinations: {df_date_combos.count():,}")

# Get daily changes (sum of all transactions per product-warehouse-date)
df_daily_changes = df_gold_transactions.groupBy(
    "product_id",
    "warehouse_id",
    F.to_date("transaction_timestamp").alias("snapshot_date")
).agg(
    F.sum("quantity_change").alias("daily_change")
)

print(f"   📊 Daily changes calculated: {df_daily_changes.count():,} records")

# Join with date combinations to get all dates (even with no transactions)
df_all_dates_changes = df_date_combos.join(
    df_daily_changes,
    on=["product_id", "warehouse_id", "snapshot_date"],
    how="left"
).withColumn(
    "daily_change",
    F.coalesce(F.col("daily_change"), F.lit(0))
)


# COMMAND ----------

print("   🔄 Calculating running inventory totals...")

# Join with initial inventory
df_with_initial = df_all_dates_changes.join(
    df_initial_inventory,
    on=["product_id", "warehouse_id"],
    how="inner"
).withColumn(
    "initial_inventory",
    F.coalesce(F.col("initial_inventory"), F.lit(0))
)

# Create window for cumulative sum
window_spec = Window.partitionBy("product_id", "warehouse_id") \
    .orderBy("snapshot_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate running inventory
df_running_inventory = df_with_initial.withColumn(
    "cumulative_change",
    F.sum("daily_change").over(window_spec)
).withColumn(
    "inventory_level",
    F.greatest(
        F.col("initial_inventory") + F.col("cumulative_change"),
        F.lit(0)
    ).cast("int")
).select(
    "product_id",
    "warehouse_id", 
    "snapshot_date",
    "initial_inventory",
    "daily_change",
    "cumulative_change",
    "inventory_level"
)


print(f"   ✅ Running inventory calculated")

# COMMAND ----------

TRANSFORMATION_TIMESTAMP = datetime.now()

# COMMAND ----------

# Join with products to get price and reorder level
df_products_metrics = df_gold_products \
    .select("product_id", "price", "reorder_level")

df_snapshots_enriched = df_running_inventory.join(
    df_products_metrics,
    on="product_id",
    how="inner"
).withColumn(
    "inventory_value",
    (F.col("inventory_level") * F.col("price"))
).withColumn(
    "days_of_supply",
    F.when(
        F.col("reorder_level") > 0,
        (F.col("inventory_level") / (F.col("reorder_level") * 0.1)).cast("int")
    ).otherwise(999)
).withColumn(
    "is_below_reorder",
    (F.col("inventory_level") < F.col("reorder_level"))
).select(
    F.col("product_id"),
    F.col("warehouse_id"),
    F.col("snapshot_date"),
    F.col("inventory_level"),
    F.round("inventory_value", 2).alias("inventory_value"),
    F.col("days_of_supply"),
    F.col("is_below_reorder"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("created_at")
)

# Assign sequential snapshot IDs
window_spec_snapshot = Window.orderBy("snapshot_date", "product_id", "warehouse_id")

df_gold_snapshots = df_snapshots_enriched.withColumn(
    "snapshot_id",
    F.row_number().over(window_spec_snapshot)
).select(
    "snapshot_id",
    "snapshot_date",
    "product_id",
    "warehouse_id",
    "inventory_level",
    F.round("inventory_value", 2).alias("inventory_value"),
    "days_of_supply",
    "is_below_reorder",
    "created_at"
)


# COMMAND ----------

display(df_gold_snapshots)

# COMMAND ----------

df_gold_snapshots.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("snapshot_date") \
    .saveAsTable(f"{catalog}.{schema}.fact_inventory_daily_snapshot")
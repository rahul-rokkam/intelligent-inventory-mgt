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

TRANSFORMATION_TIMESTAMP = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver product

# COMMAND ----------

df_bronze_mara = spark.read.table(f"{catalog}.{schema}.bronze_mara")
df_bronze_mara = spark.read.table(f"{catalog}.{schema}.bronze_mara")
df_bronze_mbew = spark.read.table(f"{catalog}.{schema}.bronze_mbew")
df_bronze_marc = spark.read.table(f"{catalog}.{schema}.bronze_marc")

# COMMAND ----------

category_mapping = {
    'HUL': 'Hull Materials',
    'PIP': 'Piping & Valves',
    'ELE': 'Electrical Systems',
    'MEC': 'Mechanical Components',
    'COA': 'Coatings & Paints',
    'FST': 'Fasteners & Hardware',
    'HVC': 'HVAC Systems',
    'AUX': 'Auxiliary Equipment'
}

# Unit of measure mapping
uom_mapping = {
    'PCE': 'piece',
    'SET': 'set',
    'KIT': 'kit',
    'PAR': 'pair',
    'EA': 'piece'
}
window_mara = Window.partitionBy("MATNR").orderBy(F.col("_ingestion_time").desc())

df_mara_deduped = df_bronze_mara \
    .withColumn("row_num", F.row_number().over(window_mara)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

duplicates_removed = df_bronze_mara.count() - df_mara_deduped.count()
print(f"   🧹 Removed {duplicates_removed:,} duplicate MARA records")

# Handle nulls and clean data
df_mara_clean = df_mara_deduped \
    .withColumn("MAKTX", F.coalesce(F.col("MAKTX"), F.lit("UNKNOWN_PRODUCT"))) \
    .withColumn("BRGEW", F.coalesce(F.col("BRGEW"), F.lit(0.0))) \
    .withColumn("MAKTX", F.trim(F.col("MAKTX")))

# COMMAND ----------

df_price_avg = df_bronze_mbew.groupBy("MATNR").agg(
    F.avg("STPRS").alias("avg_standard_price"),
    F.avg("VERPR").alias("avg_moving_price")
)

# Join MARA with pricing
df_products_joined = df_mara_clean.join(df_price_avg, on="MATNR", how="left")

# Create category mapping expression
from itertools import chain

category_map_expr = F.create_map([F.lit(x) for x in chain.from_iterable(category_mapping.items())])
uom_map_expr = F.create_map([F.lit(x) for x in chain.from_iterable(uom_mapping.items())])

window_for_key = Window.orderBy("MATNR")

# Transform to silver products
df_silver_products = df_products_joined.withColumn(
    "product_key", 
    F.concat(F.lit("PROD_"), F.lpad(F.row_number().over(window_for_key).cast("string"), 10, "0"))
).select(
    F.col("product_key"),
    F.col("MATNR").alias("material_number"),
    F.col("MAKTX").alias("product_name"),
    F.col("MAKTX").alias("product_description"),  # In reality, would come from long text
    F.concat(F.lit("HII-"), F.col("MATKL"), F.lit("-"), 
             F.substring(F.col("MATNR"), -5, 5)).alias("sku"),
    F.round(F.coalesce(F.col("avg_standard_price"), F.lit(0.0)), 2).alias("price"),
    category_map_expr[F.col("MEINS")].alias("unit_of_measure"),
    category_map_expr[F.col("MATKL")].alias("category"),
    F.col("MTART").alias("material_type"),
    F.col("MATKL").alias("material_group"),
    F.col("BRGEW").alias("gross_weight"),
    F.col("GEWEI").alias("weight_unit"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("valid_from"),
    F.lit(datetime(9999, 12, 31)).alias("valid_to"),
    F.lit(True).alias("is_current"),
    F.col("_source_system").alias("source_system"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("created_at"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("updated_at")
)

# Fill nulls in category (if mapping didn't work)
df_silver_products = df_silver_products \
    .withColumn("category", F.coalesce(F.col("category"), F.lit("Unknown"))) \
    .withColumn("unit_of_measure", F.coalesce(F.col("unit_of_measure"), F.lit("piece")))

# COMMAND ----------

display(df_silver_products)

# COMMAND ----------

df_silver_products.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.silver_products")

# COMMAND ----------

# MAGIC %md
# MAGIC # Product-Plant Relationship

# COMMAND ----------

product_key_map = df_silver_products.select("material_number", "product_key")

# Deduplicate MARC
window_marc = Window.partitionBy("MATNR", "WERKS").orderBy(F.col("_ingestion_time").desc())

df_marc_deduped = df_bronze_marc \
    .withColumn("row_num", F.row_number().over(window_marc)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

duplicates_removed_marc = df_bronze_marc.count() - df_marc_deduped.count()
print(f"   🧹 Removed {duplicates_removed_marc:,} duplicate MARC records")

# Join with product keys
df_marc_with_keys = df_marc_deduped.join(
    product_key_map,
    df_marc_deduped.MATNR == product_key_map.material_number,
    "inner"
)

# COMMAND ----------

# Transform to silver product_plants
df_silver_product_plants = df_marc_with_keys.withColumn(
    "product_plant_key",
    F.concat(F.lit("PRODPLANT_"), F.lpad(F.row_number().over(window_for_key).cast("string"), 10, "0"))
).select(
    F.col("product_plant_key"),
    F.col("product_key"),
    F.concat(F.lit("PLANT_"), F.col("WERKS")).alias("plant_key"),
    F.col("MATNR").alias("material_number"),
    F.col("WERKS").alias("plant_code"),
    F.coalesce(F.col("MINBE").cast("int"), F.lit(20)).alias("reorder_level"),
    F.coalesce(F.col("EISBE").cast("int"), F.lit(10)).alias("safety_stock"),
    F.coalesce(F.col("BSTMI").cast("int"), F.lit(1)).alias("min_order_quantity"),
    F.coalesce(F.col("BSTMA").cast("int"), F.lit(999999)).alias("max_order_quantity"),
    F.col("DISPO").alias("mrp_controller"),
    F.when(F.col("BESKZ") == "F", "External").otherwise("In-house").alias("procurement_type"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("valid_from"),
    F.lit(datetime(9999, 12, 31)).alias("valid_to"),
    F.lit(True).alias("is_current"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("created_at"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("updated_at")
)

# COMMAND ----------

display(df_silver_product_plants)

# COMMAND ----------

df_silver_product_plants.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.silver_product_plants")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Warehouses

# COMMAND ----------

# Timezone mapping - HII Facilities
timezone_mapping = {
    'VA01': 'America/New_York',
    'MS01': 'America/Chicago',
    'VA02': 'America/New_York'
}

# Manager mapping - HII Facilities
manager_mapping = {
    'VA01': 101,
    'MS01': 102,
    'VA02': 103
}

timezone_map_expr = F.create_map([F.lit(x) for x in chain.from_iterable(timezone_mapping.items())])
manager_map_expr = F.create_map([F.lit(x) for x in chain.from_iterable(manager_mapping.items())])

# COMMAND ----------

df_bronze_t001w = spark.read.table(f"{catalog}.{schema}.bronze_t001w")

# COMMAND ----------

df_silver_warehouses = df_bronze_t001w.select(
    F.concat(F.lit("PLANT_"), F.col("WERKS")).alias("warehouse_key"),
    F.col("WERKS").alias("plant_code"),
    F.col("NAME1").alias("warehouse_name"),
    F.concat_ws(", ", F.col("STRAS"), F.col("PSTLZ"), F.col("ORT01"), F.col("LAND1")).alias("location_address"),
    F.col("ORT01").alias("city"),
    F.col("REGIO").alias("region"),
    F.col("LAND1").alias("country"),
    F.col("PSTLZ").alias("postal_code"),
    timezone_map_expr[F.col("WERKS")].alias("timezone"),
    manager_map_expr[F.col("WERKS")].alias("manager_id"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("valid_from"),
    F.lit(datetime(9999, 12, 31)).alias("valid_to"),
    F.lit(True).alias("is_current"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("created_at"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("updated_at")
)

# Fill nulls with defaults
df_silver_warehouses = df_silver_warehouses \
    .withColumn("timezone", F.coalesce(F.col("timezone"), F.lit("UTC"))) \
    .withColumn("manager_id", F.coalesce(F.col("manager_id"), F.lit(100)))

# COMMAND ----------

display(df_silver_warehouses)

# COMMAND ----------

df_silver_warehouses.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.silver_warehouses")

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Inventory Transactions

# COMMAND ----------

# Movement type mapping
movement_type_mapping = {
    '101': 'inbound', '102': 'inbound', '122': 'inbound',
    '201': 'sale', '221': 'sale', '261': 'sale', '262': 'sale',
    '301': 'adjustment', '311': 'adjustment', 
    '701': 'adjustment', '702': 'adjustment', '711': 'adjustment'
}
movement_map_expr = F.create_map([F.lit(x) for x in chain.from_iterable(movement_type_mapping.items())])

# COMMAND ----------

df_bronze_mkpf = spark.read.table(f"{catalog}.{schema}.bronze_mkpf")
df_bronze_mseg = spark.read.table(f"{catalog}.{schema}.bronze_mseg")

# COMMAND ----------

# Filter active documents (not deleted)
df_mkpf_active = df_bronze_mkpf.filter(F.col("_is_deleted") == False)

deleted_count = df_bronze_mkpf.count() - df_mkpf_active.count()
print(f"   🗑️  Filtered out {deleted_count:,} deleted documents")

# COMMAND ----------

# Deduplicate MSEG based on record hash
window_mseg = Window.partitionBy("_record_hash").orderBy(F.col("_ingestion_time").desc())

df_mseg_deduped = df_bronze_mseg \
    .withColumn("row_num", F.row_number().over(window_mseg)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

mseg_duplicates = df_bronze_mseg.count() - df_mseg_deduped.count()
print(f"   🧹 Removed {mseg_duplicates:,} duplicate MSEG records")

# COMMAND ----------

# Join MKPF and MSEG
df_transactions_joined = df_mseg_deduped.join(
    df_mkpf_active.select("MBLNR", "MJAHR", "BLDAT", "BUDAT", "USNAM", "TCODE", "BKTXT", "_source_system"),
    on=["MBLNR", "MJAHR"],
    how="inner"
)

print(f"   🔗 Joined {df_transactions_joined.count():,} transactions")

# COMMAND ----------

# Create mapping for keys
product_keys = df_silver_products.select(
    F.col("material_number"),
    F.col("product_key")
)

warehouse_keys = df_silver_warehouses.select(
    F.col("plant_code"),
    F.col("warehouse_key")
)

# COMMAND ----------

# Join with dimension keys
df_with_keys = df_transactions_joined \
    .join(product_keys, df_transactions_joined.MATNR == product_keys.material_number, "inner") \
    .join(warehouse_keys, df_transactions_joined.WERKS == warehouse_keys.plant_code, "inner")

# Calculate quantity change based on debit/credit indicator
df_with_quantity = df_with_keys.withColumn(
    "quantity_change",
    F.when(F.col("SHKZG") == "H", -F.abs(F.col("MENGE").cast("int")))
     .otherwise(F.abs(F.col("MENGE").cast("int")))
)

# Parse transaction timestamp
df_with_timestamp = df_with_quantity.withColumn(
    "transaction_timestamp",
    F.to_timestamp(
        F.concat(
            F.col("CPUDT_MKPF"),
            F.lpad(F.col("CPUTM_MKPF"), 6, "0")
        ),
        "yyyyMMddHHmmss"
    ))

# Calculate days ago from current time
df_with_days_ago = df_with_timestamp.withColumn(
    "days_ago",
    F.datediff(F.lit(TRANSFORMATION_TIMESTAMP.date()), F.col("transaction_timestamp").cast("date"))
)

# Determine status based on document age
df_with_status = df_with_days_ago.withColumn(
    "status",
    F.when(F.col("days_ago") > 30, "delivered")
     .when(F.col("days_ago") > 14, 
           F.when(F.rand() < 0.9, "delivered").otherwise("shipped"))
     .when(F.col("days_ago") > 7,
           F.when(F.rand() < 0.8, "delivered").otherwise("processing"))
     .when(F.col("days_ago") > 3,
           F.when(F.rand() < 0.6, "delivered")
            .when(F.rand() < 0.9, "processing")
            .otherwise("confirmed"))
     .otherwise(
         F.when(F.rand() < 0.5, "processing")
          .when(F.rand() < 0.8, "confirmed")
          .otherwise("pending")
     )
)

# COMMAND ----------

df_with_txn_number = df_with_status.withColumn(
    "transaction_number",
    F.concat(F.lit("TRX-"), F.col("MBLNR"), F.lit("-"), F.col("ZEILE"))
)

# Build notes
df_with_notes = df_with_txn_number.withColumn(
    "notes",
    F.concat(
        F.col("SGTXT"),
        F.when(F.col("GRUND").isNotNull() & (F.col("GRUND") != ""), 
               F.concat(F.lit(" | Reason: "), F.col("GRUND")))
         .otherwise(""),
        F.when(F.col("CHARG").isNotNull() & (F.col("CHARG") != ""),
               F.concat(F.lit(" | Batch: "), F.col("CHARG")))
         .otherwise("")
    )
)

# Check for reversals
df_with_reversal = df_with_notes.withColumn(
    "is_reversal",
    F.col("BWART").isin(['102', '262'])
)

# Select final columns for silver table
df_silver_transactions = df_with_reversal.withColumn(
    "transaction_key",
    F.concat(F.lit("TXN_"), F.lpad(F.row_number().over(window_for_key).cast("string"), 15, "0"))
).select(
    F.col("transaction_key"),
    F.col("transaction_number"),
    F.col("MBLNR").alias("material_document"),
    F.col("MJAHR").alias("material_doc_year"),
    F.col("ZEILE").alias("material_doc_item"),
    F.col("product_key"),
    F.col("warehouse_key"),
    F.col("MATNR").alias("material_number"),
    F.col("WERKS").alias("plant_code"),
    F.col("LGORT").alias("storage_location"),
    F.col("quantity_change"),
    F.lit("piece").alias("unit_of_measure"),  # Simplified - would map from MEINS
    F.col("BWART").alias("movement_type"),
    movement_map_expr[F.col("BWART")].alias("transaction_type"),
    F.col("SHKZG").alias("debit_credit_indicator"),
    F.col("SOBKZ").alias("special_stock_indicator"),
    F.col("GRUND").alias("reason_code"),
    F.col("status"),
    F.trim(F.col("notes")).alias("notes"),
    F.col("transaction_timestamp"),
    F.to_date(F.col("BUDAT"), "yyyyMMdd").alias("posting_date"),
    F.to_date(F.col("BLDAT"), "yyyyMMdd").alias("document_date"),
    F.col("USNAM").alias("user_name"),
    F.col("TCODE").alias("transaction_code"),
    F.col("CHARG").alias("batch_number"),
    F.col("is_reversal"),
    F.lit(None).cast(StringType()).alias("reversed_by"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("created_at"),
    F.lit(TRANSFORMATION_TIMESTAMP).alias("updated_at")
)

# Fill null transaction types
df_silver_transactions = df_silver_transactions.withColumn(
    "transaction_type",
    F.coalesce(F.col("transaction_type"), F.lit("adjustment"))
)

# COMMAND ----------

display(df_silver_transactions)

# COMMAND ----------

df_silver_transactions.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("posting_date") \
    .saveAsTable(f"{catalog}.{schema}.silver_inventory_transactions")
# Databricks notebook source
dbutils.widgets.text("catalog", "huntington_ingalls_industries_catalog")
catalog = dbutils.widgets.get("catalog")
dbutils.widgets.text("schema", "smart_stock")
schema = dbutils.widgets.get("schema")
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

df_transactions = spark.table(f"{catalog}.{schema}.fact_inventory_transactions")
display(df_transactions)

# COMMAND ----------

df_products = spark.table(f"{catalog}.{schema}.dim_products")
df_warehouses = spark.table(f"{catalog}.{schema}.dim_warehouses")
df_historical_inventory = spark.table(f"{catalog}.{schema}.fact_inventory_daily_snapshot")

# COMMAND ----------

df_sales_history = spark.sql(
f"""
WITH all_dates AS (
    SELECT EXPLODE(
        SEQUENCE(
            (SELECT MIN(DATE(transaction_timestamp)) FROM {catalog}.{schema}.fact_inventory_transactions),
            (SELECT MAX(DATE(transaction_timestamp)) FROM {catalog}.{schema}.fact_inventory_transactions),
            INTERVAL 1 DAY
        )
    ) AS transaction_date
),
warehouse_products AS (
    SELECT DISTINCT warehouse_id, product_id FROM {catalog}.{schema}.fact_inventory_transactions
),
daily_inventory AS (
    SELECT
        warehouse_id,
        product_id,
        DATE(transaction_timestamp) AS transaction_date,
        SUM(quantity_change) AS daily_quantity_change
    FROM {catalog}.{schema}.fact_inventory_transactions
    WHERE transaction_type = 'sale'
    GROUP BY warehouse_id, product_id, DATE(transaction_timestamp)
),
calendar AS (
    SELECT
        wp.warehouse_id,
        wp.product_id,
        ad.transaction_date
    FROM warehouse_products wp
    CROSS JOIN all_dates ad
),
weekly_sales AS (
    SELECT
        c.warehouse_id,
        c.product_id,
        DATE_TRUNC('week', c.transaction_date) AS week_start,
        COALESCE(-di.daily_quantity_change, 0) AS daily_sales
    FROM calendar c
    LEFT JOIN daily_inventory di
        ON c.warehouse_id = di.warehouse_id
        AND c.product_id = di.product_id
        AND c.transaction_date = di.transaction_date
)
SELECT
    warehouse_id,
    product_id,
    week_start,
    SUM(daily_sales) AS weekly_sales
FROM weekly_sales
GROUP BY warehouse_id, product_id, week_start
ORDER BY warehouse_id, product_id, week_start
""")

# COMMAND ----------

display(df_sales_history)

# COMMAND ----------

df_sales_history.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.fact_inventory_sales_history")
# Databricks notebook source
# MAGIC %pip install mlflow-skinny=3.3.2
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Models and Prepare Data
# MAGIC Load trained models for each warehouse from MLflow and read historical sales data. Identify all unique (warehouse_id, product_id) pairs and the full range of historical week_start dates for forecasting.

# COMMAND ----------

import mlflow
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType
from datetime import datetime, timedelta
from pyspark.sql import Window
from pyspark.sql.functions import udf


# COMMAND ----------

dbutils.widgets.text("catalog", "huntington_ingalls_industries_catalog")
catalog = dbutils.widgets.get("catalog")
dbutils.widgets.text("schema", "smart_stock")
schema = dbutils.widgets.get("schema")
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

# COMMAND ----------

# Read historical sales data (limit to last 3 years for efficiency)
sales_history = spark.read.table(f"{catalog}.{schema}.fact_inventory_sales_history") \
    .filter(F.col('week_start') >= F.date_sub(F.current_date(), 7*52*3))

# Get unique (warehouse_id, product_id) pairs and all week_start dates
unique_pairs = sales_history.select('warehouse_id', 'product_id').distinct().toPandas()
unique_warehouse_ids = unique_pairs['warehouse_id'].unique()
all_weeks = sales_history.select('week_start').distinct().toPandas()['week_start'].sort_values().tolist()

# Prepare a dictionary to hold loaded models for each warehouse
warehouse_models = {}

# Load Unity Catalog model with alias 'champion' for each warehouse
for wid in unique_pairs['warehouse_id'].unique():
    model_name = f"{catalog}.{schema}.warehouse_forecast_{wid}"
    try:
        warehouse_models[wid] = mlflow.pyfunc.load_model(f"models:/{model_name}@champion")
    except Exception:
        pass

if not warehouse_models:
    raise ValueError("No models loaded. Check model registry and warehouse IDs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Forecasts for All Historical Dates Plus 6 Weeks Ahead
# MAGIC For each warehouse and product, generate predictions for all historical week_start dates plus 6 future weeks. Assemble a DataFrame with columns: warehouse_id, product_id, week_start, forecast.

# COMMAND ----------

df_weeks = pd.DataFrame({'week_start': all_weeks})
last_week = pd.to_datetime(df_weeks['week_start']).max()
future_weeks = [last_week + timedelta(weeks=i) for i in range(1, 7)]
all_forecast_weeks = pd.to_datetime(df_weeks['week_start']).tolist() + future_weeks

# Prepare forecast input DataFrame for each warehouse
forecast_results = []
warehouse_inputs = {
    wid: { "product_id": [], "week_start": [] }
    for wid in unique_warehouse_ids
}
for row in unique_pairs.itertuples():
    wid = row.warehouse_id
    pid = row.product_id
    warehouse_inputs[wid]['product_id'].extend([pid]*len(all_forecast_weeks))
    warehouse_inputs[wid]['week_start'].extend(all_forecast_weeks)

for wid in unique_warehouse_ids:
    model = warehouse_models.get(wid)
    if model is None:
        continue
    # Prepare input DataFrame for this warehouse
    input_df = pd.DataFrame(warehouse_inputs[wid])
    input_df['product_id'] = input_df['product_id'].astype('int32')
    # Predict
    try:
        preds = model.predict(input_df)
        preds['warehouse_id'] = wid
        forecast_results.append(preds)
    except Exception:
        pass

# Combine all forecasts into a single DataFrame
forecast_df = pd.concat(forecast_results, ignore_index=True)
forecast_df = forecast_df[['warehouse_id', 'product_id', 'week_start', 'prediction']].rename(columns={'prediction': 'forecast'})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write Forecasts to Table
# MAGIC Convert the pandas DataFrame to a Spark DataFrame, ensure correct types, and write to the target table, overwriting existing data.

# COMMAND ----------

# Ensure correct dtypes in pandas DataFrame
df = forecast_df.copy()
df['warehouse_id'] = df['warehouse_id'].astype(int)
df['product_id'] = df['product_id'].astype(int)
df['forecast'] = df['forecast'].astype(float)
df['week_start'] = pd.to_datetime(df['week_start'])

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Ensure week_start is timestamp
target_df = spark_df.withColumn('week_start', F.col('week_start').cast(TimestampType()))

# Write to table, overwrite mode
target_df.write.mode('overwrite').saveAsTable(f"{catalog}.{schema}.sales_forecast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Calculate Restocking Recommendations
# MAGIC Join the most recent inventory levels with the 4-week forecast sums, and recommend restocking if inventory will run out. Prepare the result for writing.

# COMMAND ----------

# 1. Get most recent inventory_level for each (warehouse_id, product_id)
inv = spark.read.table(f"{catalog}.{schema}.fact_inventory_daily_snapshot")

window = Window.partitionBy('warehouse_id', 'product_id').orderBy(F.col('snapshot_date').desc())
latest_inv = inv.withColumn('rn', F.row_number().over(window)) \
    .filter(F.col('rn') == 1) \
    .select('warehouse_id', 'product_id', F.col('inventory_level').alias('current_stock'))

# 2. Sum forecast for next 30 days for each (warehouse_id, product_id)
forecast = spark.read.table(f"{catalog}.{schema}.sales_forecast")
future_30d = forecast \
    .filter(F.col('week_start') >= F.current_date()) \
    .filter(F.col('week_start') < F.date_add(F.current_date(), 30)) \
    .groupBy('warehouse_id', 'product_id') \
    .agg(F.sum('forecast').alias('forecast_30_days'))

# 3. Join and calculate reorder logic
rec = latest_inv.join(future_30d, ['warehouse_id', 'product_id'], 'inner')

# Set reorder_quantity as forecast_30_days - current_stock if needed
rec = rec.withColumn('reorder_quantity', F.when(F.col('current_stock') < F.col('forecast_30_days'), F.col('forecast_30_days') - F.col('current_stock')).otherwise(F.lit(0)))
rec = rec.filter(F.col('reorder_quantity') > 0)

# Confidence score: high when recommendation is clear (either definitely need to reorder or definitely don't)
def confidence_udf(current_stock, forecast_30_days):
    if forecast_30_days <= 0:
        return 0.9  # High confidence that no reorder is needed if no forecast demand
    
    ratio = current_stock / forecast_30_days
    
    if ratio <= 0.5:
        # Very low stock relative to forecast - high confidence TO reorder
        return 1.0
    elif ratio <= 1.0:
        # Stock will run out - high confidence TO reorder, scaling from 0.8 to 1.0
        return 0.8 + 0.2 * (1.0 - ratio) / 0.5
    elif ratio <= 1.5:
        # Uncertain zone - medium confidence, scaling from 0.4 to 0.8
        return 0.4 + 0.4 * abs(1.25 - ratio) / 0.25
    elif ratio <= 2.5:
        # Good buffer - increasing confidence NOT to reorder, scaling from 0.6 to 0.9
        return 0.6 + 0.3 * (ratio - 1.5) / 1.0
    else:
        # Plenty of stock - high confidence NOT to reorder
        return 0.95

confidence_score_udf = udf(confidence_udf, DoubleType())
rec = rec.withColumn('confidence_score', confidence_score_udf(F.col('current_stock'), F.col('forecast_30_days')))
rec = rec.join(
    spark.read.table(f"{catalog}.{schema}.dim_products")
        .select('product_id', F.col('reorder_level').alias('reorder_point')),
    on='product_id',
    how='left'
)

# Select and order columns as per schema
rec_final = rec.select(
    'product_id',
    'warehouse_id',
    'current_stock',
    'forecast_30_days',
    'reorder_point',
    'reorder_quantity',
    F.round('confidence_score', 2).alias('confidence_score')
)

display(rec_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write Restocking Recommendations to Table
# MAGIC Write the restocking recommendations DataFrame to the lars_dev.forecast.inventory_forecast table, overwriting any existing data. This will make the recommendations available for downstream use.

# COMMAND ----------

rec_final \
.withColumn("forecast_id", F.monotonically_increasing_id()) \
.withColumn("status", F.lit("active")) \
.withColumn("last_updated", F.lit(datetime.now())) \
.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f"{catalog}.{schema}.fact_inventory_forecast")

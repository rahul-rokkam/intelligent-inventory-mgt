# Databricks notebook source
# MAGIC %pip install mlflow-skinny=3.3.2
# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta
from pyspark.sql.functions import col
import mlflow.pyfunc
import mlflow
import tempfile
import uuid
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

# COMMAND ----------

dbutils.widgets.text("catalog", "huntington_ingalls_industries_catalog")
catalog = dbutils.widgets.get("catalog")
dbutils.widgets.text("schema", "smart_stock")
schema = dbutils.widgets.get("schema")
dbutils.widgets.text("ml_artifact_volume", "ml_experiments")
ml_artifact_volume = dbutils.widgets.get("ml_artifact_volume")
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

experiment_name = "/Shared/smartstock_v2_sales_forecasting"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare Data for Modeling (last 3 years, all warehouse/product pairs)
# MAGIC We'll load the last 3 years of data from the sales_history table, identify all unique (warehouse_id, product_id) pairs, and organize the data for modeling. We'll use Spark for efficient data handling, then convert to Pandas for modeling.

# COMMAND ----------

# Define the time window: last 3 years from today
end_date = datetime.now()
start_date = end_date - timedelta(days=3*365)

# Load the sales_history table
sales_history = spark.table(f"{catalog}.{schema}.fact_inventory_sales_history")

# Filter for last 3 years
sales_history_recent = sales_history.filter(col("week_start").between(start_date, end_date))

# Identify all unique (warehouse_id, product_id) pairs
unique_pairs = (sales_history_recent
    .select("warehouse_id", "product_id")
    .distinct()
    .toPandas()
    .values.tolist()
)

print(f"Number of unique (warehouse_id, product_id) pairs: {len(unique_pairs)}")
display(sales_history_recent.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Train Simple Forecasting Models for Each (warehouse_id, product_id) Pair
# MAGIC We'll iterate over each (warehouse_id, product_id) pair, extract its time series, train an Exponential Smoothing model, store the model, and collect metrics and plots. We'll organize models by warehouse for the next step.

# COMMAND ----------

os.makedirs("tmp", exist_ok=True)

def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = y_true != 0
    if not np.any(mask):
        return np.nan
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

# Convert Spark DataFrame to Pandas for modeling
pdf = sales_history_recent.toPandas()

# Ensure week_start is datetime
pdf['week_start'] = pd.to_datetime(pdf['week_start'])

# Prepare storage
warehouse_models = {}
warehouse_metrics = {}
warehouse_plots = {}
product_train_ranges = {}  # Store training date range for each product

for wid, pid in unique_pairs:
    ts = pdf[(pdf['warehouse_id'] == wid) & (pdf['product_id'] == pid)].sort_values('week_start')
    if len(ts) < 10:
        continue  # skip very short series
    y = ts.set_index('week_start')['weekly_sales']
    # Simple train/test split: last 8 weeks as test
    train, test = y.iloc[:-8], y.iloc[-8:]
    # Fit Exponential Smoothing
    try:
        model = ExponentialSmoothing(
            train, trend='add', seasonal=None, freq="W-MON"
        ).fit()
        # Store the training date range for predict()
        product_train_ranges.setdefault(wid, {})[pid] = (train.index.values[0], train.index.values[-1])
        # For metrics, use predict for the test period
        pred = model.predict(start=test.index.values[0], end=test.index.values[-1])
    except Exception as e:
        print(f"Model failed for warehouse {wid}, product {pid}: {e}")
        continue
    # Metrics
    mae = mean_absolute_error(test, pred)
    rmse = np.sqrt(mean_squared_error(test, pred))
    mape = mean_absolute_percentage_error(test, pred)
    # Store
    warehouse_models.setdefault(wid, {})[pid] = model
    warehouse_metrics.setdefault(wid, {})[pid] = {'mae': mae, 'rmse': rmse, 'mape': mape}
    # Plot
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(train.index.values, train, label='Train')
    ax.plot(test.index.values, test, label='Test', marker='o')
    ax.plot(test.index.values, pred, label='Predict', marker='x')
    ax.set_title(f'Warehouse {wid}, Product {pid}')
    ax.legend()
    ax.grid(True)
    plt.tight_layout()
    # Save plot to memory
    plot_path = f"tmp/plot_w{wid}_p{pid}.png"
    fig.savefig(plot_path)
    plt.close(fig)
    warehouse_plots.setdefault(wid, {})[pid] = plot_path

print(f"Trained models for {len(warehouse_models)} warehouses.")
print("Sample metrics:", list(warehouse_metrics.items())[:1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Define a Custom MLflow Pyfunc Model for Each Warehouse
# MAGIC We'll define a custom MLflow Pyfunc class that holds all product models for a warehouse. The class will route predictions to the correct product model based on input product_id. We'll also prepare the model for MLflow logging.

# COMMAND ----------

class WarehouseForecastModel(mlflow.pyfunc.PythonModel):
    def __init__(self, product_models):
        """
        product_models: dict of {product_id: fitted_model}
        """
        self.product_models = product_models

    def predict(self, context, model_input):
        """
        model_input: pd.DataFrame with columns ['product_id', 'week_start']
        Returns: pd.DataFrame with columns ['product_id', 'week_start', 'prediction']
        """
        results = []
        for _, row in model_input.iterrows():
            pid = row['product_id']
            week = pd.to_datetime(row['week_start'])
            model = self.product_models.get(pid)
            if model is not None:
                pred = model.predict(start=week, end=week)
                results.append({'product_id': pid, 'week_start': week, 'prediction': float(pred.iloc[0])})
            else:
                results.append({'product_id': pid, 'week_start': week, 'prediction': None})
        return pd.DataFrame(results)

# Example usage (not run):
# warehouse_model = WarehouseForecastModel(product_models=warehouse_models[wid], product_train_ranges=product_train_ranges[wid])
# warehouse_model.predict(None, pd.DataFrame({'product_id': [pid], 'week_start': [pd.Timestamp('2024-01-01')]}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Log Models, Plots, and Metrics to MLflow for Each Warehouse
# MAGIC For each warehouse, log the custom Pyfunc model to MLflow, including all product models, metrics, and forecast plots as artifacts. We'll use cloudpickle to serialize the product models dictionary. We'll also log metrics and plots for each product as MLflow artifacts.

# COMMAND ----------

experiment = mlflow.get_experiment_by_name(experiment_name)
if experiment is None:
    mlflow.create_experiment(
        experiment_name,
        #artifact_location=f"dbfs:/Volumes/{catalog}/{schema}/{ml_artifact_volume}/sales_forecasting/"
    )
mlflow.set_experiment(experiment_name)

for wid, product_models in warehouse_models.items():
    sample_pid = next(iter(product_models))
    example_input = pdf.loc[
        (pdf['warehouse_id'] == wid) & (pdf['product_id'] == sample_pid),
        ['product_id', 'week_start'],
    ].sort_values('week_start')
    warehouse_model = WarehouseForecastModel(product_models=product_models)
    example_output = warehouse_model.predict(None, example_input)
    signature = infer_signature(example_input, example_output)
    training_range = product_train_ranges[wid][sample_pid]
    training_start = pd.to_datetime(training_range[0]).strftime("%Y-%m-%d")
    training_end = pd.to_datetime(training_range[1]).strftime("%Y-%m-%d")

    run_name = f"warehouse_{wid}_" + str(uuid.uuid4())[:8]
    with mlflow.start_run(run_name=run_name) as run:
        model_name = f"warehouse_model_{wid}"
        mlflow.pyfunc.log_model(
            name=model_name,
            python_model=WarehouseForecastModel(product_models=product_models),
            signature=signature,
            input_example=example_input,
        )
        mlflow.log_params({
            "training_start": training_start,
            "training_end": training_end,
        })
        mlflow.set_tag("warehouse_id", wid)
        for pid, metrics in warehouse_metrics[wid].items():
            mlflow.log_metric(f"mae_product_{pid}", metrics['mae'])
            mlflow.log_metric(f"rmse_product_{pid}", metrics['rmse'])
            mlflow.log_metric(f"mape_product_{pid}", metrics['mape'])
        with tempfile.TemporaryDirectory() as tmpdir:
            for pid, plot_path in warehouse_plots[wid].items():
                dest_path = os.path.join(tmpdir, os.path.basename(plot_path))

    print(f"Logged MLflow run for warehouse {wid}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Register Models to Unity Catalog
# MAGIC We need to register each warehouse's MLflow model to Unity Catalog under the catalog lars_dev and schema forecast. We'll search for the latest run/model for each warehouse, then use mlflow.register_model with the correct UC path. We'll ensure model names are unique (e.g., warehouse_forecast_{warehouse_id}). We'll also print the registration status for each model.

# COMMAND ----------

client = MlflowClient()

# Robustly get experiment_id
exp_obj = mlflow.get_experiment_by_name(experiment_name)
if exp_obj is None:
    raise Exception(f"Experiment {experiment_name} not found")
experiment_id = exp_obj.experiment_id

for wid in warehouse_models.keys():
    # Find the latest run for this warehouse
    filter_string = f"tags.warehouse_id = '{wid}'"
    runs = client.search_runs(
        experiment_ids=[experiment_id],
        filter_string=filter_string,
        order_by=["attributes.start_time DESC"],
        max_results=1
    )
    if not runs:
        print(f"No MLflow run found for warehouse {wid}")
        continue
    run = runs[0]
    # The model artifact path is warehouse_model_{wid}
    model_uri = f"models:/{run.outputs.model_outputs[0].model_id}"
    model_name = f"{catalog}.{schema}.warehouse_forecast_{wid}"
    try:
        result = mlflow.register_model(model_uri, model_name)
        client.set_registered_model_alias(model_name, "champion", result.version)
        print(f"Registered model for warehouse {wid} as {model_name} and set it as champion model.")
    except Exception as e:
        print(f"Failed to register model for warehouse {wid}: {e}")
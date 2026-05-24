from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta


# ---------------------------------------------------------
# DAG DEFAULT CONFIG
# ---------------------------------------------------------

default_args = {
    "owner": "jay-soni",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------
# DAG CONFIGURATION
# ---------------------------------------------------------

with DAG(
    dag_id="Your_dag_id",
    default_args=default_args,
    description="Retail Sales Forecasting Pipeline using GCS, Dataflow, BigQuery and BigQuery ML",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["gcp", "retail", "data-engineering", "bigquery", "forecast"],
) as dag:

    # ---------------------------------------------------------
    # STEP 1 - START PIPELINE
    # ---------------------------------------------------------

    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command='echo "Starting Retail Sales Forecast Pipeline..."',
    )

    # ---------------------------------------------------------
    # STEP 2 - RUN DATAFLOW PIPELINE
    # ---------------------------------------------------------
    # Replace the below command with your actual Dataflow job command

    run_dataflow_pipeline = BashOperator(
        task_id="run_dataflow_pipeline",
        bash_command="""
        python3 /home/airflow/gcs/dags/dataflow/pipeline.py \
        --runner DataflowRunner \
        --project your-project-name \
        --region us-central1 \
        --temp_location gs://Your-Bucket_Name/temp \
        --staging_location gs://Your-Bucket_Name/staging \
        --input gs://Your-Bucket_Name/raw/retail_sales.csv \
        --output_table your-project-id:retail.sales_data
        """,
    )

    # ---------------------------------------------------------
    # STEP 3 - CREATE MONTHLY SALES TABLE
    # ---------------------------------------------------------

    create_monthly_sales = BigQueryInsertJobOperator(
        task_id="create_monthly_sales",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE retail.monthly_sales AS
                SELECT
                  EXTRACT(YEAR FROM Order_Date) AS year,
                  EXTRACT(MONTH FROM Order_Date) AS month,
                  ROUND(SUM(Sales),2) AS total_sales,
                  ROUND(SUM(Profit),2) AS total_profit,
                  SUM(Quantity) AS total_quantity,
                  ROUND(AVG(Profit_Margin),4) AS avg_profit_margin
                FROM retail.sales_data
                GROUP BY year, month
                ORDER BY year, month;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # ---------------------------------------------------------
    # STEP 4 - CREATE YEARLY SALES TABLE
    # ---------------------------------------------------------

    create_yearly_sales = BigQueryInsertJobOperator(
        task_id="create_yearly_sales",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE retail.yearly_sales AS
                SELECT
                  EXTRACT(YEAR FROM Order_Date) AS year,
                  ROUND(SUM(Sales),2) AS total_sales,
                  ROUND(SUM(Profit),2) AS total_profit,
                  SUM(Quantity) AS total_quantity,
                  ROUND(AVG(Profit_Margin),4) AS avg_profit_margin
                FROM retail.sales_data
                GROUP BY year
                ORDER BY year;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # ---------------------------------------------------------
    # STEP 5 - CREATE PROFIT ANALYSIS TABLE
    # ---------------------------------------------------------

    create_profit_analysis = BigQueryInsertJobOperator(
        task_id="create_profit_analysis",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE retail.profit_analysis AS
                SELECT
                  Order_ID,
                  Order_Date,
                  Sales,
                  Profit,
                  Profit_Margin,
                  CASE
                    WHEN Profit > 0 THEN 'Profitable'
                    ELSE 'Loss'
                  END AS profit_status
                FROM retail.sales_data;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # ---------------------------------------------------------
    # STEP 6 - CREATE HIGH VALUE ORDERS TABLE
    # ---------------------------------------------------------

    create_high_value_orders = BigQueryInsertJobOperator(
        task_id="create_high_value_orders",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE retail.high_value_orders AS
                SELECT
                  Order_ID,
                  Order_Date,
                  Sales,
                  Profit,
                  Quantity
                FROM retail.sales_data
                WHERE Sales > 200
                ORDER BY Sales DESC;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # ---------------------------------------------------------
    # STEP 7 - CREATE FORECAST DATA TABLE
    # ---------------------------------------------------------

    create_forecast_dataset = BigQueryInsertJobOperator(
        task_id="create_forecast_dataset",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE retail.sales_forecast_data AS
                SELECT
                  DATE_TRUNC(Order_Date, MONTH) AS month,
                  SUM(Sales) AS total_sales
                FROM retail.sales_data
                GROUP BY month
                ORDER BY month;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # ---------------------------------------------------------
    # STEP 8 - CREATE BIGQUERY ML MODEL
    # ---------------------------------------------------------

    create_forecast_model = BigQueryInsertJobOperator(
        task_id="create_forecast_model",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE MODEL retail.sales_forecast_model
                OPTIONS(
                  MODEL_TYPE='ARIMA_PLUS',
                  TIME_SERIES_TIMESTAMP_COL='month',
                  TIME_SERIES_DATA_COL='total_sales'
                ) AS

                SELECT *
                FROM retail.sales_forecast_data;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # ---------------------------------------------------------
    # STEP 9 - GENERATE FORECAST RESULTS
    # ---------------------------------------------------------

    generate_forecast_results = BigQueryInsertJobOperator(
        task_id="generate_forecast_results",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE retail.sales_forecast_results AS
                SELECT *
                FROM ML.FORECAST(
                  MODEL retail.sales_forecast_model,
                  STRUCT(6 AS horizon, 0.8 AS confidence_level)
                );
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # ---------------------------------------------------------
    # STEP 10 - END PIPELINE
    # ---------------------------------------------------------

    end_pipeline = BashOperator(
        task_id="end_pipeline",
        bash_command='echo "Retail Sales Forecast Pipeline Completed Successfully!"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ---------------------------------------------------------
    # DAG WORKFLOW ORDER
    # ---------------------------------------------------------

    start_pipeline >> run_dataflow_pipeline

    run_dataflow_pipeline >> [
        create_monthly_sales,
        create_yearly_sales,
        create_profit_analysis,
        create_high_value_orders,
        create_forecast_dataset,
    ]

    create_forecast_dataset >> create_forecast_model
    create_forecast_model >> generate_forecast_results

    [
        create_monthly_sales,
        create_yearly_sales,
        create_profit_analysis,
        create_high_value_orders,
        generate_forecast_results,
    ] >> end_pipeline

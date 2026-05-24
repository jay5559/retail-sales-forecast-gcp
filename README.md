# Retail Sales Forecasting Pipeline on Google Cloud Platform

End-to-end cloud-native retail analytics and forecasting project built using Google Cloud Platform services including Cloud Storage, Apache Beam, Dataflow, BigQuery, BigQuery ML, Cloud Composer (Apache Airflow), and Looker Studio.


## Project Overview

Retail organizations generate large volumes of transactional sales data every day.
Without proper data engineering pipelines and analytics systems, it becomes difficult to:

- Monitor sales performance
- Analyze profit trends
- Track order growth
- Forecast future revenue
- Build business intelligence dashboards
- Make data-driven decisions

This project demonstrates how to build a scalable and production-style retail analytics platform using Google Cloud Platform (GCP).

The pipeline automates:

- Data ingestion
- ETL transformation
- Data warehousing
- Analytics table generation
- Machine learning forecasting
- Workflow orchestration
- Dashboard visualization


## Business Problem

Retail businesses need a centralized analytics platform to:

- Store and process sales transactions
- Generate monthly and yearly sales insights
- Identify profitable vs loss-making orders
- Forecast future sales trends
- Visualize KPIs and business metrics

Traditional systems struggle with scalability, automation, and advanced analytics.

This project solves these challenges using modern cloud-native data engineering architecture.


## Project Goals

The objectives of this project are:

- Build scalable ETL pipelines using Apache Beam
- Process retail sales data on Google Cloud
- Store analytics-ready data in BigQuery
- Generate SQL-based business insights
- Forecast sales using BigQuery ML
- Orchestrate workflows using Apache Airflow
- Create interactive dashboards in Looker Studio

## Architecture Diagram
![image](https://github.com/jay5559/retail-sales-forecast-gcp/blob/main/image/data_diagram.png)


### Tech Stack

| Technology            | Purpose                         |
| --------------------- | ------------------------------- |
| Google Cloud Storage  | Raw data storage                |
| Apache Beam           | ETL transformations             |
| Google Cloud Dataflow | Managed Beam execution          |
| BigQuery              | Cloud data warehouse            |
| BigQuery ML           | Machine learning forecasting    |
| Cloud Composer        | Workflow orchestration          |
| Apache Airflow        | DAG scheduling & automation     |
| Looker Studio         | Dashboard visualization         |
| Python                | ETL & orchestration development |
| SQL                   | Analytics queries               |


## Dataset Information

The dataset contains retail sales transaction records including:

- Order ID
- Order Date
- Ship Date
- Sales
- Quantity
- Profit
- Profit Margin


## End-to-End Pipeline Workflow

### 1. Data Ingestion Layer

Raw retail CSV files are uploaded into Google Cloud Storage.

#### Services Used
- Google Cloud Storage (GCS)
#### Features
- Centralized raw storage
- Scalable object storage
- Cloud-native ingestion layer

### 2. ETL Transformation Layer

Apache Beam ETL pipeline performs:
- CSV parsing
- Data cleaning
- Invalid row handling
- Date formatting
- Numeric transformations
- Schema standardization

#### Services Used
- Apache Beam
- Google Cloud Dataflow

### 3. Data Warehouse Layer

Cleaned data is loaded into BigQuery tables.

### Main Table
retail.sales_data

## Analytics Tables

#### Monthly Sales Analysis
```
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
```

#### Yearly Performance Analysis
```
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
```

#### Profitability Analysis
```
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
```

#### High Value Orders Analysis
```
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
```

## Machine Learning Forecasting

**BigQuery ML is used to forecast future retail sales trends.**

#### Forecast Dataset
```
CREATE OR REPLACE TABLE retail.sales_forecast_data AS
SELECT
  DATE_TRUNC(Order_Date, MONTH) AS month,
  SUM(Sales) AS total_sales
FROM retail.sales_data
GROUP BY month
ORDER BY month;
```

#### Forecast Model
```
CREATE OR REPLACE MODEL retail.sales_forecast_model
OPTIONS(
  MODEL_TYPE='ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL='month',
  TIME_SERIES_DATA_COL='total_sales'
) AS

SELECT *
FROM retail.sales_forecast_data;
```

#### Forecast Prediction
```
SELECT *
FROM ML.FORECAST(
  MODEL retail.sales_forecast_model,
  STRUCT(6 AS horizon, 0.8 AS confidence_level)
);
```

## Workflow Orchestration Using Cloud Composer

**Apache Airflow DAGs are used to orchestrate the entire pipeline automatically.**

### Workflow Includes
- Trigger ETL pipeline
- Load cleaned data into BigQuery
- Generate analytics tables
- Execute BigQuery ML forecasting
- Store forecast results
- Automate daily scheduling

### Services Used
- Cloud Composer
- Apache Airflow

## Dashboard & Visualization

**Interactive business dashboards are built using Looker Studio.**

### KPI Cards
- Total Sales
- Total Profit
- Total Orders
- Average Profit Margin

### Analytics Charts
- Monthly Sales Trend
- Profit Trend
- Quantity Sold Trend
- Profit vs Loss Orders
- High Value Orders Table
- Forecast Sales Trend

### Sample Business Insights

**Key insights generated from the analytics pipeline:**

- Monthly sales show seasonal demand trends
- Certain periods generate lower profit margins
- High-value orders contribute significantly to revenue
- Forecasting predicts future sales growth
- Profitability trends help identify operational performance

### BigQuery Optimization

###### Warehouse tables are optimized using:
- Partitioning
- Aggregation tables
- Query optimization techniques

###### Example:
```
CREATE OR REPLACE TABLE retail.sales_data_partitioned
PARTITION BY DATE(Order_Date)
AS
SELECT *
FROM retail.sales_data;
```

## Cloud Composer DAG Workflow
```
START
  │
  ▼
Run Apache Beam Dataflow Pipeline
  │
  ▼
Load Cleaned Data into BigQuery
  │
  ▼
Create Analytics Tables
  │
  ▼
Generate Forecast Dataset
  │
  ▼
Train BigQuery ML Model
  │
  ▼
Generate Forecast Results
  │
  ▼
END
```

## Skills Demonstrated

###### This project demonstrates practical experience with:
- Cloud Data Engineering
- Apache Beam Pipelines
- Google Cloud Dataflow
- BigQuery Data Warehousing
- SQL Analytics
- BigQuery ML Forecasting
- Apache Airflow Orchestration
- Cloud Composer
- Dashboard Development
- Cloud Architecture Design

## Future Improvements

###### Future enhancements planned:
- Real-time streaming with Pub/Sub
- Advanced ML models using Vertex AI
- CI/CD deployment pipeline
- dbt transformation layer
- Multi-source ingestion pipelines
- Automated monitoring & alerting

## Author
### Jay Soni 
###### (GCP Certified Professional Data Engineer)
###### Cloud Data Engineering Project on Google Cloud Platform

## License
###### This project is intended for educational and portfolio purposes only.

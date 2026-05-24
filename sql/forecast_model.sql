CREATE OR REPLACE TABLE retail.sales_forecast_data AS
SELECT
  DATE_TRUNC(Order_Date, MONTH) AS month,
  SUM(Sales) AS total_sales
FROM retail.sales_data
GROUP BY month
ORDER BY month;

CREATE OR REPLACE MODEL retail.sales_forecast_model
OPTIONS(
  MODEL_TYPE='ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL='month',
  TIME_SERIES_DATA_COL='total_sales'
) AS

SELECT *
FROM retail.sales_forecast_data;

SELECT *
FROM ML.FORECAST(
  MODEL retail.sales_forecast_model,
  STRUCT(6 AS horizon, 0.8 AS confidence_level)
);
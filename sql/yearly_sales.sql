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
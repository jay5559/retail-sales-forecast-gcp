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
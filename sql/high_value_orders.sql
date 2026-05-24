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
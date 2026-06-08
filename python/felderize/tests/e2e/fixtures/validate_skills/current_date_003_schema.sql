-- rule: current_date
-- spark: CURRENT_DATE — today's date
-- feldera: CAST(NOW() AS DATE)
CREATE TABLE sales_transactions (
  transaction_id INT,
  product_name STRING,
  sale_amount DECIMAL(10,2),
  transaction_date DATE
);

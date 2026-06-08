-- rule: select_alias_chain
-- spark: SELECT col1 AS a, a + 1 AS b FROM t — reference an earlier SELECT alias in a later expression
-- feldera: Expand the alias reference: SELECT col1 AS a, col1 + 1 AS b FROM t — replace alias references with the original expression
CREATE TABLE sales_data (product_id INT, base_price DECIMAL(10,2), quantity INT);

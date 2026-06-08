-- rule: select_alias_chain
-- spark: SELECT col1 AS a, a + 1 AS b FROM t — reference an earlier SELECT alias in a later expression
-- feldera: Expand the alias reference: SELECT col1 AS a, col1 + 1 AS b FROM t — replace alias references with the original expression
CREATE OR REPLACE TEMP VIEW price_calculation_v1 AS SELECT base_price AS unit_cost, unit_cost * 1.15 AS markup_price, markup_price * quantity AS total_revenue FROM sales_data;

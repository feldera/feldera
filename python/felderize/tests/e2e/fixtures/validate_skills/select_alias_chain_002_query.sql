-- rule: select_alias_chain
-- spark: SELECT col1 AS a, a + 1 AS b FROM t — reference an earlier SELECT alias in a later expression
-- feldera: Expand the alias reference: SELECT col1 AS a, col1 + 1 AS b FROM t — replace alias references with the original expression
CREATE OR REPLACE TEMP VIEW temp_conversion_v2 AS SELECT location, celsius AS temp_c, temp_c * 9 / 5 + 32 AS temp_f, temp_f - temp_c AS difference FROM temperature_log;

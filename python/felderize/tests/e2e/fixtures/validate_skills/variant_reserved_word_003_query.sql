-- rule: variant_reserved_word
-- spark: Column named 'variant' — Spark SQL allows 'variant' as an unquoted identifier
-- feldera: "variant" is a reserved keyword in Feldera — quote the column name as "variant" in both CREATE TABLE DDL and all query references
CREATE OR REPLACE TEMP VIEW format_summary_v3 AS SELECT variant, COUNT(*) AS num_formats, SUM(cnt) AS total FROM data_formats_3 GROUP BY variant;

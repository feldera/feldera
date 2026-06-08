-- rule: variant_reserved_word
-- spark: Column named 'variant' — Spark SQL allows 'variant' as an unquoted identifier
-- feldera: "variant" is a reserved keyword in Feldera — quote the column name as "variant" in both CREATE TABLE DDL and all query references
CREATE TABLE data_formats_3 (format_id INT, format_name STRING, variant STRING, cnt INT);

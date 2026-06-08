-- rule: variant_reserved_word
-- spark: Column named 'variant' — Spark SQL allows 'variant' as an unquoted identifier
-- feldera: "variant" is a reserved keyword in Feldera — quote the column name as "variant" in both CREATE TABLE DDL and all query references
CREATE OR REPLACE TEMP VIEW structured_files_v2 AS SELECT file_id, filename FROM file_records_2 WHERE variant = 'structured';

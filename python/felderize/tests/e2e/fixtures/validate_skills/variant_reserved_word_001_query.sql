-- rule: variant_reserved_word
-- spark: Column named 'variant' — Spark SQL allows 'variant' as an unquoted identifier
-- feldera: "variant" is a reserved keyword in Feldera — quote the column name as "variant" in both CREATE TABLE DDL and all query references
CREATE OR REPLACE TEMP VIEW format_list_v1 AS SELECT id, name, variant FROM format_types_1;

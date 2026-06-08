-- rule: variant_reserved_word
-- spark: Column named 'variant' — Spark SQL allows 'variant' as an unquoted identifier
-- feldera: "variant" is a reserved keyword in Feldera — quote the column name as "variant" in both CREATE TABLE DDL and all query references
CREATE TABLE file_records_2 (file_id INT, filename STRING, variant STRING, size INT);

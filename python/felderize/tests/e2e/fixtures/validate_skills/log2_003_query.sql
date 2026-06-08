-- rule: log2
-- spark: log2(x) — base-2 logarithm
-- feldera: LOG(x, 2)
CREATE OR REPLACE TEMP VIEW file_size_analysis_v3 AS SELECT file_id, file_name, size_bytes, log2(size_bytes) AS log2_size_bits FROM data_sizes WHERE size_bytes > 0;

CREATE OR REPLACE TEMP VIEW gpt138_split_part AS
SELECT
  entry_id,
  source,
  SPLIT_PART(raw_path, '/', 1) AS first_segment,
  SPLIT_PART(raw_path, '/', 2) AS second_segment,
  SPLIT_PART(raw_path, '/', -1) AS last_segment
FROM log_entries;

-- rule: split_3arg
-- spark: split(str, delimiter, limit) — split with a limit on the number of parts returned
-- feldera: UNSUPPORTED — Feldera SPLIT does not support the 3-argument form with a limit parameter. Drop the limit if result is equivalent, otherwise mark unsupported.
CREATE OR REPLACE TEMP VIEW split_3arg_v3 AS SELECT id, filepath, split(filepath, '/', 4) AS path_parts FROM path_data;

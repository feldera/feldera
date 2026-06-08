-- rule: split_3arg
-- spark: split(str, delimiter, limit) — split with a limit on the number of parts returned
-- feldera: UNSUPPORTED — Feldera SPLIT does not support the 3-argument form with a limit parameter. Drop the limit if result is equivalent, otherwise mark unsupported.
CREATE OR REPLACE TEMP VIEW split_3arg_v2 AS SELECT id, split(config_str, '|', 3) AS tokens FROM config_data WHERE id > 0;

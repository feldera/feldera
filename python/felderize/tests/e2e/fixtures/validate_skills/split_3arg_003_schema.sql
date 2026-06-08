-- rule: split_3arg
-- spark: split(str, delimiter, limit) — split with a limit on the number of parts returned
-- feldera: UNSUPPORTED — Feldera SPLIT does not support the 3-argument form with a limit parameter. Drop the limit if result is equivalent, otherwise mark unsupported.
CREATE TABLE path_data (id INT, filepath STRING);

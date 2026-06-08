-- rule: split_func
-- spark: split(str, delimiter) — split a string by a literal delimiter and return an array of substrings
-- feldera: SPLIT(str, delimiter) — same function; Feldera treats delimiter as a literal string (not regex). Use only plain alphanumeric delimiters in tests.
CREATE OR REPLACE TEMP VIEW csv_records_split AS SELECT record_id, split(data, ';') AS fields FROM csv_records;

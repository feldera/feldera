-- rule: split_func
-- spark: split(str, delimiter) — split a string by a literal delimiter and return an array of substrings
-- feldera: SPLIT(str, delimiter) — same function; Feldera treats delimiter as a literal string (not regex). Use only plain alphanumeric delimiters in tests.
CREATE TABLE file_paths (id INT, full_path STRING);

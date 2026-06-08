-- rule: split_func
-- spark: split(str, delimiter) — split a string by a literal delimiter and return an array of substrings
-- feldera: SPLIT(str, delimiter) — same function; Feldera treats delimiter as a literal string (not regex). Use only plain alphanumeric delimiters in tests.
CREATE OR REPLACE TEMP VIEW file_paths_split AS SELECT id, split(full_path, '/') AS path_components FROM file_paths WHERE full_path IS NOT NULL;

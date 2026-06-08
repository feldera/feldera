-- rule: overlay
-- spark: overlay(str placing repl from pos for len) — replace substring
-- feldera: OVERLAY(str PLACING repl FROM pos FOR len)
CREATE TABLE log_entries_v3 (log_id INT, entry STRING);

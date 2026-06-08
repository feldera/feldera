-- rule: overlay
-- spark: overlay(str placing repl from pos for len) — replace substring
-- feldera: OVERLAY(str PLACING repl FROM pos FOR len)
CREATE OR REPLACE TEMP VIEW overlay_result_v3 AS SELECT log_id, overlay(entry placing '[SENSITIVE]' from 15 for 8) as cleaned_entry FROM log_entries_v3;

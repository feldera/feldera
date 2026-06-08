-- rule: overlay
-- spark: overlay(str placing repl from pos for len) — replace substring
-- feldera: OVERLAY(str PLACING repl FROM pos FOR len)
CREATE OR REPLACE TEMP VIEW overlay_result_v1 AS SELECT doc_id, overlay(content placing 'REDACTED' from 10 for 5) as masked_content FROM document_v1;

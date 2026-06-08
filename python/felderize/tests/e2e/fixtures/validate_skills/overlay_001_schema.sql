-- rule: overlay
-- spark: overlay(str placing repl from pos for len) — replace substring
-- feldera: OVERLAY(str PLACING repl FROM pos FOR len)
CREATE TABLE document_v1 (doc_id INT, content STRING);

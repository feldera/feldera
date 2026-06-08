-- rule: contains_str
-- spark: contains(s, sub) — true if s contains sub
-- feldera: POSITION(sub IN s) > 0
CREATE OR REPLACE TEMP VIEW doc_search_v3 AS SELECT doc_id, title, body, contains(body, 'error') AS mentions_error, contains(title, 'report') AS is_report FROM document_content;

-- rule: contains_str
-- spark: contains(s, sub) — true if s contains sub
-- feldera: POSITION(sub IN s) > 0
CREATE TABLE document_content (doc_id INT, title STRING, body STRING);

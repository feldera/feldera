-- rule: contains_str
-- spark: contains(s, sub) — true if s contains sub
-- feldera: POSITION(sub IN s) > 0
CREATE TABLE email_log (email_id INT, sender STRING, subject STRING);

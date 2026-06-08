-- rule: contains_str
-- spark: contains(s, sub) — true if s contains sub
-- feldera: POSITION(sub IN s) > 0
CREATE OR REPLACE TEMP VIEW email_filter_v2 AS SELECT email_id, sender, subject, contains(subject, 'urgent') AS is_urgent FROM email_log;

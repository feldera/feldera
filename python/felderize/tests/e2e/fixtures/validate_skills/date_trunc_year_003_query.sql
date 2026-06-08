-- rule: date_trunc_year
-- spark: date_trunc('YEAR', ts) — truncate timestamp to year
-- feldera: TIMESTAMP_TRUNC(ts, YEAR)
CREATE OR REPLACE TEMP VIEW yearly_audit_view_v3 AS SELECT audit_id, action_type, user_name, date_trunc('YEAR', action_timestamp) AS year_truncated FROM audit_trail;

-- rule: date_subtract_int
-- spark: date_col - integer_expr — subtract an integer number of days from a DATE column
-- feldera: date_col - integer_expr * INTERVAL '1' DAY — Feldera does not support DATE minus integer directly; multiply by INTERVAL '1' DAY
CREATE OR REPLACE TEMP VIEW notice_dates_v3 AS SELECT milestone_id, deadline - days_advance AS notice_date, deadline FROM project_milestones WHERE days_advance >= 0;

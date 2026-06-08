-- rule: date_format_func
-- spark: date_format(d, 'yyyy-MM-dd HH:mm:ss') — format date/timestamp as string using Java pattern
-- feldera: FORMAT_TIMESTAMP(strftime_fmt, d) — arg order reversed; translate Java pattern to strftime (yyyy→%Y, MM→%m, dd→%d, HH→%H, mm→%M, ss→%S). For date-only inputs use FORMAT_DATE(strftime_fmt, d)
CREATE OR REPLACE TEMP VIEW audit_formatted AS SELECT
  audit_id,
  action,
  user_name,
  date_format(action_timestamp, 'yyyy-MM-dd') AS action_date,
  date_format(action_timestamp, 'HH:mm:ss') AS action_time
FROM audit_trail;

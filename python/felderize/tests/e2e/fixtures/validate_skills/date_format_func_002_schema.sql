-- rule: date_format_func
-- spark: date_format(d, 'yyyy-MM-dd HH:mm:ss') — format date/timestamp as string using Java pattern
-- feldera: FORMAT_TIMESTAMP(strftime_fmt, d) — arg order reversed; translate Java pattern to strftime (yyyy→%Y, MM→%m, dd→%d, HH→%H, mm→%M, ss→%S). For date-only inputs use FORMAT_DATE(strftime_fmt, d)
CREATE TABLE transaction_records (
  trans_id INT,
  amount DECIMAL(10, 2),
  trans_date DATE
);

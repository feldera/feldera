-- rule: add_months
-- spark: add_months(date, n) — add n months to a date
-- feldera: date + INTERVAL 'n' MONTH  (literal n) or  date + n * INTERVAL '1' MONTH  (column n)
CREATE TABLE contract_dates (contract_id INT, agreement_date DATE, extension_months INT);

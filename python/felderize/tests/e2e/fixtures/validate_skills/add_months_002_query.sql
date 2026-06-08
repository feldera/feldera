-- rule: add_months
-- spark: add_months(date, n) — add n months to a date
-- feldera: date + INTERVAL 'n' MONTH  (literal n) or  date + n * INTERVAL '1' MONTH  (column n)
CREATE OR REPLACE TEMP VIEW contract_dates_v2 AS SELECT contract_id, agreement_date, add_months(agreement_date, extension_months) AS expiration_date FROM contract_dates WHERE extension_months >= 0;

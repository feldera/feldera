CREATE OR REPLACE TEMP VIEW val06_gross_amounts AS
SELECT
  row_id,
  zip_with(
    prices,
    taxes,
    (p, t) -> coalesce(p, CAST(0 AS DECIMAL(10,2))) + coalesce(t, CAST(0 AS DECIMAL(10,2)))
  ) AS gross_amounts
FROM collection_events;

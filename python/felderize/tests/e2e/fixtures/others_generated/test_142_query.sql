CREATE OR REPLACE TEMP VIEW gpt142_exp AS
SELECT
  period_id,
  label,
  growth_rate,
  EXP(growth_rate) AS growth_factor
FROM compound_growth;

CREATE OR REPLACE TEMP VIEW bm86_region_priority_pivot AS
SELECT * FROM (
  SELECT region, priority, case_id FROM priority_cases
) src PIVOT (COUNT(case_id) FOR priority IN ('LOW', 'MEDIUM', 'HIGH'));

CREATE VIEW group-by_002 AS
SELECT 1 from (
  SELECT 1 AS z,
  MIN(a.x)
  FROM (select 1 as x) a
  WHERE false
) b
where b.z != b.z;

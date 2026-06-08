CREATE OR REPLACE TEMP VIEW bm526_creative_cube AS
SELECT
  region,
  channel,
  grouping_id(region, channel) AS gid,
  SUM(units) AS total_units
FROM creative_sales
GROUP BY CUBE(region, channel);

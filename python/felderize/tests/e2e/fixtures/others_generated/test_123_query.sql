CREATE OR REPLACE TEMP VIEW bm525_creative_grouping_sets AS
SELECT
  region,
  channel,
  SUM(amount) AS revenue,
  GROUPING(region) AS g_region,
  GROUPING(channel) AS g_channel
FROM creative_sales
GROUP BY GROUPING SETS ((region, channel), (region), ());

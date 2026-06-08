CREATE OR REPLACE TEMP VIEW bm36_region_channel_matrix AS
SELECT r.region, c.channel FROM regions r CROSS JOIN channels c;

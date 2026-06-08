CREATE OR REPLACE TEMP VIEW fe009_edge_unpivot_nulls AS
SELECT *
FROM feldera_edge_wide_sensors
UNPIVOT INCLUDE NULLS (
  reading FOR sensor IN (s1, s2, s3)
);

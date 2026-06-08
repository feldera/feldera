CREATE OR REPLACE TEMP VIEW fe015_edge_unpivot_exclude AS
SELECT *
FROM feldera_edge_wide_four
UNPIVOT EXCLUDE NULLS (
  val FOR metric IN (m1, m2, m3, m4)
);

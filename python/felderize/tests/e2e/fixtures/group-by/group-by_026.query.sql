CREATE VIEW group-by_026 AS
SELECT
  histogram_numeric(col, 2) as histogram_2,
  histogram_numeric(col, 3) as histogram_3,
  histogram_numeric(col, 5) as histogram_5,
  histogram_numeric(col, 10) as histogram_10
FROM VALUES
 (1), (2), (3), (4), (5), (6), (7), (8), (9), (10),
 (11), (12), (13), (14), (15), (16), (17), (18), (19), (20),
 (21), (22), (23), (24), (25), (26), (27), (28), (29), (30),
 (31), (32), (33), (34), (35), (3), (37), (38), (39), (40),
 (41), (42), (43), (44), (45), (46), (47), (48), (49), (50) AS tab(col);

CREATE VIEW union_003 AS
SELECT 1 AS x,
       col
FROM   (SELECT col AS col
        FROM (SELECT p1.col AS col
              FROM   p1 CROSS JOIN p2
              UNION ALL
              SELECT col
              FROM p3) T1) T2;

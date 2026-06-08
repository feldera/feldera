CREATE VIEW group-by_045 AS
SELECT c * 2 AS d
FROM (
         SELECT if(b > 1, 1, b) AS c
         FROM (
                  SELECT if(a < 0, 0, a) AS b
                  FROM VALUES (-1), (1), (2) AS t1(a)
              ) t2
         GROUP BY b
     ) t3
GROUP BY c;

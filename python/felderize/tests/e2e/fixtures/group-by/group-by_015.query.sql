CREATE VIEW group-by_015 AS
SELECT k,
       Every(v) AS every
FROM   test_agg
WHERE  k = 2
       AND v IN (SELECT Every(v)
                 FROM   test_agg
                 WHERE  k = 1)
GROUP  BY k;

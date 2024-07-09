CREATE VIEW q8 AS
SELECT P.id, P.name, P.starttime
FROM (
  SELECT P.id, P.name,
         TUMBLE_START(P.date_time, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(P.date_time, INTERVAL '10' SECOND) AS endtime
  FROM person P
  GROUP BY P.id, P.name, TUMBLE(P.date_time, INTERVAL '10' SECOND)
) P
JOIN (
  SELECT A.seller,
         TUMBLE_START(A.date_time, INTERVAL '10' SECOND) AS starttime,
         TUMBLE_END(A.date_time, INTERVAL '10' SECOND) AS endtime
  FROM auction A
  GROUP BY A.seller, TUMBLE(A.date_time, INTERVAL '10' SECOND)
) A
ON P.id = A.seller AND P.starttime = A.starttime AND P.endtime = A.endtime;
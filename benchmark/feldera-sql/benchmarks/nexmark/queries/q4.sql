CREATE VIEW q4 AS
SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.date_time BETWEEN A.date_time AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;
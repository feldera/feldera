CREATE TABLE side_input (
  date_time TIMESTAMP,
  key BIGINT,
  value VARCHAR
) WITH ('connectors' = '[{
  "transport": {
      "name": "datagen",
      "config": {
        "plan": [
          {
            "limit": 100,
            "fields": {
              "date_time": { "range": [1724444408000, 4102444800000] },
              "key": { "range": [0, 100] }
            }
          }
        ]
      }
    }
}]');

CREATE VIEW Q13 AS
SELECT
    B.auction,
    B.bidder,
    B.price,
    B.date_time,
    S.value
FROM (SELECT *, date_time as p_time, mod(auction, 10000) as mod FROM bid) B
LEFT ASOF JOIN side_input AS S
MATCH_CONDITION B.p_time >= S.date_time
ON B.mod = S.key;

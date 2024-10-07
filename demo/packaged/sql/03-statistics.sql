-- Statistics (statistics)
--
-- Statistics (e.g., mean, standard deviation, etc.) being continuously
-- updated as data is produced by a generator source.
--

-- Table with as data source randomly generated numbers at a rate of 1000/s
CREATE TABLE numbers (
    num DOUBLE
) WITH (
    'connectors' = '[{
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [{
                    "rate": 1000,
                    "fields": {
                        "num": {
                            "range": [0, 1000],
                            "strategy": "uniform"
                        }
                    }
                }]
            }
        }
    }]'
);

-- Calculate statistics over the stream of incoming numbers
CREATE MATERIALIZED VIEW stats AS (
    SELECT COUNT(*), MIN(num), MAX(num), AVG(num), SUM(num), STDDEV_SAMP(num), STDDEV_POP(num)
    FROM   numbers
);

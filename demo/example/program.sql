-- Example input
CREATE TABLE example (
    id INT NOT NULL PRIMARY KEY
);

-- Example output
CREATE VIEW example_count AS (
    SELECT COUNT(*) AS num_rows FROM example
);

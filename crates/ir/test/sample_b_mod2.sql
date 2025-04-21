CREATE TABLE example ( id INT NOT NULL PRIMARY KEY, mod INT ) WITH ('connectors' = '[{ "name": "c", "transport": { "name": "datagen", "config": { "plan": [{ "limit": 1 }] } } }]');
CREATE VIEW example_count WITH ('connectors' = '[{ "name": "c", "transport": { "name": "file_output", "config": { "path": "bla" } }, "format": { "name": "csv" } }]') AS ( SELECT COUNT(*) AS num_rows FROM example );

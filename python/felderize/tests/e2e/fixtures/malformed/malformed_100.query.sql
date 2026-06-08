CREATE VIEW malformed_100_a AS
SELECT id, k, val FROM base WHERE k = 'k9;

CREATE VIEW malformed_100 AS
SELECT id, val FROM malformed_100_a;

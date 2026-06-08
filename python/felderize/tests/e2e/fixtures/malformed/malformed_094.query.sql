CREATE VIEW malformed_094_a AS
SELECT id, k, val FROM base WHERE k = 'k3;

CREATE VIEW malformed_094 AS
SELECT id, val FROM malformed_094_a;

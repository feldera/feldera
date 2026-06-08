CREATE VIEW malformed_095_a AS
SELECT id, k, val FROM base WHERE k = 'k4;

CREATE VIEW malformed_095 AS
SELECT id, val FROM malformed_095_a;

CREATE VIEW malformed_093_a AS
SELECT id, k, val FROM base WHERE k = 'k2;

CREATE VIEW malformed_093 AS
SELECT id, val FROM malformed_093_a;

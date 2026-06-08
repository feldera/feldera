CREATE VIEW malformed_096_a AS
SELECT id, k, val FROM base WHERE k = 'k5;

CREATE VIEW malformed_096 AS
SELECT id, val FROM malformed_096_a;

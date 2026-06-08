CREATE VIEW malformed_097_a AS
SELECT id, k, val FROM base WHERE k = 'k6;

CREATE VIEW malformed_097 AS
SELECT id, val FROM malformed_097_a;

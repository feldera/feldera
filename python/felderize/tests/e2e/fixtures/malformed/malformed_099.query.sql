CREATE VIEW malformed_099_a AS
SELECT id, k, val FROM base WHERE k = 'k8;

CREATE VIEW malformed_099 AS
SELECT id, val FROM malformed_099_a;

CREATE VIEW malformed_073_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_073_b AS
SELECT id, val FROM base WHERE val > 2;

CREATE VIEW malformed_073 AS
SELECT id, val
FROM malformed_073_a JOIN malformed_073_b ON malformed_073_a.id = malformed_073_b.id;

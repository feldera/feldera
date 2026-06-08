CREATE VIEW malformed_079_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_079_b AS
SELECT id, val FROM base WHERE val > 8;

CREATE VIEW malformed_079 AS
SELECT id, val
FROM malformed_079_a JOIN malformed_079_b ON malformed_079_a.id = malformed_079_b.id;

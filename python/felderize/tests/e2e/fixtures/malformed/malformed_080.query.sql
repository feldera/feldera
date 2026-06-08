CREATE VIEW malformed_080_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_080_b AS
SELECT id, val FROM base WHERE val > 9;

CREATE VIEW malformed_080 AS
SELECT id, val
FROM malformed_080_a JOIN malformed_080_b ON malformed_080_a.id = malformed_080_b.id;

CREATE VIEW malformed_075_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_075_b AS
SELECT id, val FROM base WHERE val > 4;

CREATE VIEW malformed_075 AS
SELECT id, val
FROM malformed_075_a JOIN malformed_075_b ON malformed_075_a.id = malformed_075_b.id;

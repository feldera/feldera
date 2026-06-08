CREATE VIEW malformed_076_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_076_b AS
SELECT id, val FROM base WHERE val > 5;

CREATE VIEW malformed_076 AS
SELECT id, val
FROM malformed_076_a JOIN malformed_076_b ON malformed_076_a.id = malformed_076_b.id;

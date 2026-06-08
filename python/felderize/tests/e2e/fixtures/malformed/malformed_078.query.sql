CREATE VIEW malformed_078_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_078_b AS
SELECT id, val FROM base WHERE val > 7;

CREATE VIEW malformed_078 AS
SELECT id, val
FROM malformed_078_a JOIN malformed_078_b ON malformed_078_a.id = malformed_078_b.id;

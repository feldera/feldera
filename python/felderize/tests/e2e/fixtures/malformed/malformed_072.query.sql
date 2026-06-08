CREATE VIEW malformed_072_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_072_b AS
SELECT id, val FROM base WHERE val > 1;

CREATE VIEW malformed_072 AS
SELECT id, val
FROM malformed_072_a JOIN malformed_072_b ON malformed_072_a.id = malformed_072_b.id;

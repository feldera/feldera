CREATE VIEW malformed_071_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_071_b AS
SELECT id, val FROM base WHERE val > 0;

CREATE VIEW malformed_071 AS
SELECT id, val
FROM malformed_071_a JOIN malformed_071_b ON malformed_071_a.id = malformed_071_b.id;

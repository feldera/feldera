CREATE VIEW malformed_074_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_074_b AS
SELECT id, val FROM base WHERE val > 3;

CREATE VIEW malformed_074 AS
SELECT id, val
FROM malformed_074_a JOIN malformed_074_b ON malformed_074_a.id = malformed_074_b.id;

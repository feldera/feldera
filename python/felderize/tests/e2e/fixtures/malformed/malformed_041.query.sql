CREATE VIEW malformed_041_a AS
SELECT id, val FROM base WHERE val >= 0;

CREATE VIEW malformed_041 AS
SELECT id, val FROM malformed_041_a;

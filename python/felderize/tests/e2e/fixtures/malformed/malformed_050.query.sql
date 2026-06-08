CREATE VIEW malformed_050_a AS
SELECT id, val FROM base WHERE val >= 9;

CREATE VIEW malformed_050 AS
SELECT id, val FROM malformed_050_a;

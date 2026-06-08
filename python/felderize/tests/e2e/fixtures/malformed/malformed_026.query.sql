CREATE VIEW malformed_026_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_026_b AS
SELECT id, val FROM malformed_026_a WHERE val > 5;

CREATE VIEW malformed_026 AS
SELECT id, val FROM malformed_026_b;

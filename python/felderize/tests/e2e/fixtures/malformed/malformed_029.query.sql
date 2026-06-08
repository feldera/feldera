CREATE VIEW malformed_029_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_029_b AS
SELECT id, val FROM malformed_029_a WHERE val > 8;

CREATE VIEW malformed_029 AS
SELECT id, val FROM malformed_029_b;

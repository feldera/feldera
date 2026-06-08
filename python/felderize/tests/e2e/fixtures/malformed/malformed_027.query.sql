CREATE VIEW malformed_027_a AS
SELECT id k, val FROM base;

CREATE VIEW malformed_027_b AS
SELECT id, val FROM malformed_027_a WHERE val > 6;

CREATE VIEW malformed_027 AS
SELECT id, val FROM malformed_027_b;

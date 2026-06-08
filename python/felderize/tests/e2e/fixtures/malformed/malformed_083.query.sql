CREATE VIEW malformed_083_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_083 AS
SELECT id, val FROM malformed_083_a WHERE

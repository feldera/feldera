CREATE VIEW malformed_084_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_084 AS
SELECT id, val FROM malformed_084_a WHERE

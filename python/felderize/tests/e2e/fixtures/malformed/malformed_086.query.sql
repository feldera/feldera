CREATE VIEW malformed_086_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_086 AS
SELECT id, val FROM malformed_086_a WHERE

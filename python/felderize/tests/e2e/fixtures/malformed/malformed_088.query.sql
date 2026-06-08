CREATE VIEW malformed_088_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_088 AS
SELECT id, val FROM malformed_088_a WHERE

CREATE VIEW malformed_082_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_082 AS
SELECT id, val FROM malformed_082_a WHERE

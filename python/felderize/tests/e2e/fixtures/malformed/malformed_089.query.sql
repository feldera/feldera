CREATE VIEW malformed_089_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_089 AS
SELECT id, val FROM malformed_089_a WHERE

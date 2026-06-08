CREATE VIEW malformed_090_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_090 AS
SELECT id, val FROM malformed_090_a WHERE

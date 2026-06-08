CREATE VIEW malformed_081_a AS
SELECT id, val FROM base;

CREATE VIEW malformed_081 AS
SELECT id, val FROM malformed_081_a WHERE

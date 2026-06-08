CREATE VIEW malformed_035_a AS
SELECT id, k, val FROM base;

CREATE VIEW malformed_035 AS
SELECT x.id, x.val FROM malformed_035_missing_4 x;

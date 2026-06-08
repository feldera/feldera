CREATE VIEW malformed_098_a AS
SELECT id, k, val FROM base WHERE k = 'k7;

CREATE VIEW malformed_098 AS
SELECT id, val FROM malformed_098_a;

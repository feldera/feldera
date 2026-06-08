CREATE VIEW malformed_005_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_005_b AS
SELECT a.id, a.val, g.note
FROM malformed_005_a a JOIN ghost_table_4 g ON a.id = g.id;

CREATE VIEW malformed_005 AS
SELECT id, val, note FROM malformed_005_b;

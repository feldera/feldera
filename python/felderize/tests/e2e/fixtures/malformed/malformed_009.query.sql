CREATE VIEW malformed_009_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_009_b AS
SELECT a.id, a.val, g.note
FROM malformed_009_a a JOIN ghost_table_8 g ON a.id = g.id;

CREATE VIEW malformed_009 AS
SELECT id, val, note FROM malformed_009_b;

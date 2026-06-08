CREATE VIEW malformed_010_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_010_b AS
SELECT a.id, a.val, g.note
FROM malformed_010_a a JOIN ghost_table_9 g ON a.id = g.id;

CREATE VIEW malformed_010 AS
SELECT id, val, note FROM malformed_010_b;

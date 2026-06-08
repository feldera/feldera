CREATE VIEW malformed_004_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_004_b AS
SELECT a.id, a.val, g.note
FROM malformed_004_a a JOIN ghost_table_3 g ON a.id = g.id;

CREATE VIEW malformed_004 AS
SELECT id, val, note FROM malformed_004_b;

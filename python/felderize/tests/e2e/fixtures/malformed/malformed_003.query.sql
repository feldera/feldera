CREATE VIEW malformed_003_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_003_b AS
SELECT a.id, a.val, g.note
FROM malformed_003_a a JOIN ghost_table_2 g ON a.id = g.id;

CREATE VIEW malformed_003 AS
SELECT id, val, note FROM malformed_003_b;

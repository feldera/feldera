CREATE VIEW malformed_002_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_002_b AS
SELECT a.id, a.val, g.note
FROM malformed_002_a a JOIN ghost_table_1 g ON a.id = g.id;

CREATE VIEW malformed_002 AS
SELECT id, val, note FROM malformed_002_b;

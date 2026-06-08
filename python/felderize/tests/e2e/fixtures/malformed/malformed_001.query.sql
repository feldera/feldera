CREATE VIEW malformed_001_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_001_b AS
SELECT a.id, a.val, g.note
FROM malformed_001_a a JOIN ghost_table_0 g ON a.id = g.id;

CREATE VIEW malformed_001 AS
SELECT id, val, note FROM malformed_001_b;

CREATE VIEW malformed_008_a AS
SELECT id, k, val FROM base WHERE val > 0;

CREATE VIEW malformed_008_b AS
SELECT a.id, a.val, g.note
FROM malformed_008_a a JOIN ghost_table_7 g ON a.id = g.id;

CREATE VIEW malformed_008 AS
SELECT id, val, note FROM malformed_008_b;

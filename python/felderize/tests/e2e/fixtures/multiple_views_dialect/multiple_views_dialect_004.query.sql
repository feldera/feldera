CREATE VIEW multiple_views_dialect_004_raw AS
SELECT item_id, name, note AS note_raw, qty AS qty_raw
FROM items WHERE item_id > 0;

CREATE VIEW multiple_views_dialect_004_filled AS
SELECT item_id, name,
       nvl(note_raw, 'n/a') AS note,
       nvl(qty_raw, 0) AS qty,
       nvl2(note_raw, 1, 0) AS had_note
FROM multiple_views_dialect_004_raw;

CREATE VIEW multiple_views_dialect_004_agg AS
SELECT note, SUM(qty) AS total_qty, SUM(had_note) AS with_note, COUNT(*) AS n
FROM multiple_views_dialect_004_filled GROUP BY note;

CREATE VIEW multiple_views_dialect_004_ranked AS
SELECT note, total_qty, with_note, n
FROM multiple_views_dialect_004_agg WHERE total_qty >= 16;

CREATE VIEW multiple_views_dialect_004 AS
SELECT note, total_qty, n FROM multiple_views_dialect_004_ranked;

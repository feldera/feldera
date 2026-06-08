CREATE VIEW multiple_views_dialect_014_raw AS
SELECT cid, raw_code, label FROM codes WHERE cid > 0;

CREATE VIEW multiple_views_dialect_014_norm AS
SELECT cid,
       lpad(raw_code, 5, '0') AS code,
       initcap(label) AS label,
       length(raw_code) AS raw_len
FROM multiple_views_dialect_014_raw;

CREATE VIEW multiple_views_dialect_014_agg AS
SELECT label, COUNT(*) AS n, MAX(code) AS top_code
FROM multiple_views_dialect_014_norm GROUP BY label;

CREATE VIEW multiple_views_dialect_014_kept AS
SELECT label, n, top_code FROM multiple_views_dialect_014_agg WHERE n >= 1;

CREATE VIEW multiple_views_dialect_014 AS
SELECT label, n, top_code FROM multiple_views_dialect_014_kept;

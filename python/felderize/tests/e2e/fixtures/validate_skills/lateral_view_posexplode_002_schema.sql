-- rule: lateral_view_posexplode
-- spark: LATERAL VIEW posexplode(arr) t AS pos, item — unnest with 0-based position index
-- feldera: UNNEST(arr) WITH ORDINALITY AS t(item, pos) — Feldera ordinal is 1-based, comes after value
CREATE TABLE student_scores (student_id INT, subject STRING, scores ARRAY<INT>);

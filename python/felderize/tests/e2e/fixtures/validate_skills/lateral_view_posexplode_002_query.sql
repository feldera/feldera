-- rule: lateral_view_posexplode
-- spark: LATERAL VIEW posexplode(arr) t AS pos, item — unnest with 0-based position index
-- feldera: UNNEST(arr) WITH ORDINALITY AS t(item, pos) — Feldera ordinal is 1-based, comes after value
CREATE OR REPLACE TEMP VIEW student_scores_indexed AS SELECT student_id, subject, pos, item as score FROM student_scores LATERAL VIEW posexplode(scores) t AS pos, item;

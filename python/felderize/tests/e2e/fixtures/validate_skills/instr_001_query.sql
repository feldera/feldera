-- rule: instr
-- spark: instr(str, substr) — 1-based position of first occurrence (same as LOCATE but arg order reversed)
-- feldera: POSITION(substr IN str)
CREATE OR REPLACE TEMP VIEW product_search_v1 AS SELECT id, name, instr(name, 'phone') AS phone_pos FROM product_names WHERE instr(name, 'phone') > 0;

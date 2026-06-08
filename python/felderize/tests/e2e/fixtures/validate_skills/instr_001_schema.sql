-- rule: instr
-- spark: instr(str, substr) — 1-based position of first occurrence (same as LOCATE but arg order reversed)
-- feldera: POSITION(substr IN str)
CREATE TABLE product_names (id INT, name STRING);

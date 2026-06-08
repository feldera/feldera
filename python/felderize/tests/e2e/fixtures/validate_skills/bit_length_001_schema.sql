-- rule: bit_length
-- spark: BIT_LENGTH(s) — bit length of string
-- feldera: OCTET_LENGTH(s) * 8
CREATE TABLE text_samples_1 (id INT, content STRING);

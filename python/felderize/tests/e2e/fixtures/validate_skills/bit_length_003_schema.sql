-- rule: bit_length
-- spark: BIT_LENGTH(s) — bit length of string
-- feldera: OCTET_LENGTH(s) * 8
CREATE TABLE encoded_text_3 (id INT, text_value STRING, notes STRING);

-- rule: octet_length
-- spark: OCTET_LENGTH(s) — number of bytes in string (same as LENGTH for ASCII; differs for multi-byte UTF-8)
-- feldera: OCTET_LENGTH(s) — same function, supported directly in Feldera
CREATE TABLE text_data_v3 (record_id INT, title STRING, description STRING);

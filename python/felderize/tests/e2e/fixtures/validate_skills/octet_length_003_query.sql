-- rule: octet_length
-- spark: OCTET_LENGTH(s) — number of bytes in string (same as LENGTH for ASCII; differs for multi-byte UTF-8)
-- feldera: OCTET_LENGTH(s) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW octet_test_v3 AS SELECT record_id, title, description, OCTET_LENGTH(title) AS title_bytes, OCTET_LENGTH(description) AS desc_bytes FROM text_data_v3 WHERE OCTET_LENGTH(title) <= 10 AND OCTET_LENGTH(description) > 0 ORDER BY record_id;

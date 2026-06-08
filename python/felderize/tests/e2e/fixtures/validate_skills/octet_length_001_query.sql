-- rule: octet_length
-- spark: OCTET_LENGTH(s) — number of bytes in string (same as LENGTH for ASCII; differs for multi-byte UTF-8)
-- feldera: OCTET_LENGTH(s) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW octet_test_v1 AS SELECT id, content, OCTET_LENGTH(content) AS byte_count FROM messages_v1 WHERE OCTET_LENGTH(content) > 0 ORDER BY id;

-- rule: octet_length
-- spark: OCTET_LENGTH(s) — number of bytes in string (same as LENGTH for ASCII; differs for multi-byte UTF-8)
-- feldera: OCTET_LENGTH(s) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW octet_test_v2 AS SELECT log_id, message, level, OCTET_LENGTH(message) AS msg_bytes, OCTET_LENGTH(level) AS level_bytes FROM logs_v2 WHERE OCTET_LENGTH(message) >= 5 ORDER BY log_id;

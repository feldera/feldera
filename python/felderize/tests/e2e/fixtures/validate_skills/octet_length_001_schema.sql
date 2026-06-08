-- rule: octet_length
-- spark: OCTET_LENGTH(s) — number of bytes in string (same as LENGTH for ASCII; differs for multi-byte UTF-8)
-- feldera: OCTET_LENGTH(s) — same function, supported directly in Feldera
CREATE TABLE messages_v1 (id INT, content STRING);

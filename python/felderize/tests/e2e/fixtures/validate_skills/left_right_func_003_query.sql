-- rule: left_right_func
-- spark: LEFT(s, n) — first n characters; RIGHT(s, n) — last n characters
-- feldera: LEFT(s, n) / RIGHT(s, n) — both work identically in Feldera, no translation needed
CREATE OR REPLACE TEMP VIEW txn_parsing AS SELECT txn_id, LEFT(transaction_code, 5) AS bank_code, RIGHT(transaction_code, 3) AS type_code, amount FROM transaction_ids WHERE transaction_code IS NOT NULL;

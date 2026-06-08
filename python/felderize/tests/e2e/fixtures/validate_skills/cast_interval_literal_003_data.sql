INSERT INTO subscription_records VALUES (501, 'Alice Johnson', CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'premium');
INSERT INTO subscription_records VALUES (502, 'Bob Smith', CAST('2024-01-10 00:00:00' AS TIMESTAMP), 'basic');
INSERT INTO subscription_records VALUES (503, 'Carol White', CAST('2024-01-15 00:00:00' AS TIMESTAMP), 'premium');
INSERT INTO subscription_records VALUES (504, 'David Brown', CAST('2024-01-20 00:00:00' AS TIMESTAMP), 'premium');
INSERT INTO subscription_records VALUES (505, 'Eve Davis', CAST('2024-02-01 00:00:00' AS TIMESTAMP), 'basic');
INSERT INTO renewal_notices VALUES (1, 501, CAST('2024-01-31 12:00:00' AS TIMESTAMP), 1);
INSERT INTO renewal_notices VALUES (2, 503, CAST('2024-02-14 12:00:00' AS TIMESTAMP), 1);
INSERT INTO renewal_notices VALUES (3, 504, CAST('2024-02-19 12:00:00' AS TIMESTAMP), 1);

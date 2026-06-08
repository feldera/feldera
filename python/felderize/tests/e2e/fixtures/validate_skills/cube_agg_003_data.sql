INSERT INTO transaction_log VALUES
('Premium', 'Credit', CAST(250.50 AS DECIMAL(10,2))),
('Premium', 'Debit', CAST(150.75 AS DECIMAL(10,2))),
('Premium', 'PayPal', CAST(300.00 AS DECIMAL(10,2))),
('Standard', 'Credit', CAST(100.25 AS DECIMAL(10,2))),
('Standard', 'Debit', CAST(75.50 AS DECIMAL(10,2))),
('Standard', 'PayPal', CAST(125.00 AS DECIMAL(10,2))),
('Guest', 'Credit', CAST(50.00 AS DECIMAL(10,2)));

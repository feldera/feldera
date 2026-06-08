INSERT INTO order_logs VALUES
(1000, 'John Doe', CAST('2024-01-15 10:30:00' AS TIMESTAMP), 'shipped'),
(1001, 'Jane Smith', CAST('2024-01-15 11:45:00' AS TIMESTAMP), 'pending'),
(1002, 'Bob Johnson', CAST('2024-01-16 09:15:00' AS TIMESTAMP), 'delivered'),
(1003, 'Alice Brown', CAST('2024-01-16 14:20:00' AS TIMESTAMP), NULL),
(1004, 'Charlie Wilson', CAST('2024-01-17 08:00:00' AS TIMESTAMP), 'pending'),
(1005, 'Emma Davis', NULL, 'shipped'),
(1006, 'Frank Miller', CAST('2024-01-18 16:45:00' AS TIMESTAMP), 'cancelled');

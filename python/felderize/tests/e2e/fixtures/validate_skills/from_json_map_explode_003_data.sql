INSERT INTO sensor_data_003 VALUES
(1000, CAST('2024-01-15 10:30:00' AS TIMESTAMP), '{"status": "active", "temperature": "22.5"}'),
(1001, CAST('2024-01-15 10:31:00' AS TIMESTAMP), '{"status": "active", "humidity": "45"}'),
(1002, CAST('2024-01-15 10:32:00' AS TIMESTAMP), '{"status": "warning", "pressure": "1013", "location": "room_a"}'),
(1003, CAST('2024-01-15 10:33:00' AS TIMESTAMP), '{"status": "idle"}'),
(1004, CAST('2024-01-15 10:34:00' AS TIMESTAMP), '{"status": "error", "error_id": "E001", "retry_count": "3", "last_error_time": "2024-01-15"}');

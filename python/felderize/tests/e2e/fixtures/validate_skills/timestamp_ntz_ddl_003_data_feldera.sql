INSERT INTO task_schedule_t3 VALUES
  (1, 'data_sync', CAST('2024-01-15 06:00:00' AS TIMESTAMP), CAST('2024-01-15 06:30:00' AS TIMESTAMP), 'completed'),
  (2, 'backup', CAST('2024-01-15 07:00:00' AS TIMESTAMP), NULL, 'pending'),
  (3, 'cleanup', CAST('2024-01-15 08:00:00' AS TIMESTAMP), CAST('2024-01-15 08:15:00' AS TIMESTAMP), 'completed'),
  (4, 'report', CAST('2024-01-15 09:00:00' AS TIMESTAMP), NULL, 'in_progress'),
  (5, 'archive', CAST('2024-01-15 10:00:00' AS TIMESTAMP), CAST('2024-01-15 10:45:00' AS TIMESTAMP), 'completed'),
  (6, 'verify', CAST('2024-01-15 11:00:00' AS TIMESTAMP), CAST('2024-01-15 11:20:00' AS TIMESTAMP), 'completed');

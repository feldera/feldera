INSERT INTO event_metadata VALUES (1001, map('timestamp', '2024-01-15T10:30:00', 'source', 'web', 'user_agent', 'Chrome'));
INSERT INTO event_metadata VALUES (1002, map('endpoint', '/api/users', 'method', 'POST', 'status', '200', 'duration', '45ms'));
INSERT INTO event_metadata VALUES (1003, map('error_code', '404', 'error_msg', 'Not Found'));
INSERT INTO event_metadata VALUES (1004, map('region', 'US', 'country', 'USA', 'city', 'NYC', 'ip', '192.168.1.1', 'isp', 'Verizon'));
INSERT INTO event_metadata VALUES (1005, map('session_id', 'sess_abc123', 'device', 'mobile', 'os', 'iOS'));

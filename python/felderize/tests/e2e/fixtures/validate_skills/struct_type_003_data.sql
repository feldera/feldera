INSERT INTO event_log VALUES
(1001, 'Tech Conference', named_struct('timestamp', CAST('2024-01-15 10:30:00' AS TIMESTAMP), 'location', 'New York', 'attendee_count', 250)),
(1002, 'Workshop Series', named_struct('timestamp', CAST('2024-01-20 14:00:00' AS TIMESTAMP), 'location', 'Boston', 'attendee_count', 45)),
(1003, 'Meetup Group', named_struct('timestamp', CAST('2024-01-25 18:00:00' AS TIMESTAMP), 'location', 'San Francisco', 'attendee_count', 120)),
(1004, 'Panel Discussion', named_struct('timestamp', CAST('2024-02-01 11:30:00' AS TIMESTAMP), 'location', 'Chicago', 'attendee_count', 75)),
(1005, 'Training Session', named_struct('timestamp', CAST('2024-02-05 09:00:00' AS TIMESTAMP), 'location', 'Seattle', 'attendee_count', 30));

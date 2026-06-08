-- rule: lateral_view_explode
-- spark: LATERAL VIEW explode(arr) t AS item — unnest array column into rows
-- feldera: CROSS JOIN LATERAL UNNEST(arr) AS t(item)  or  FROM t, UNNEST(arr) AS u(item)
CREATE TABLE event_attendees_003 (event_id INT, event_name STRING, attendee_emails ARRAY<STRING>);

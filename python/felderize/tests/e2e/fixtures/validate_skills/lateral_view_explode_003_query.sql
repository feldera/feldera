-- rule: lateral_view_explode
-- spark: LATERAL VIEW explode(arr) t AS item — unnest array column into rows
-- feldera: CROSS JOIN LATERAL UNNEST(arr) AS t(item)  or  FROM t, UNNEST(arr) AS u(item)
CREATE OR REPLACE TEMP VIEW attendee_list_v3 AS SELECT event_id, event_name, email FROM event_attendees_003 LATERAL VIEW explode(attendee_emails) a AS email;

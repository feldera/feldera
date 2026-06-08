-- rule: json_array_len
-- spark: json_array_length(json_str) — length of a JSON array string
-- feldera: CARDINALITY(CAST(PARSE_JSON(json_str) AS VARIANT ARRAY))
CREATE OR REPLACE TEMP VIEW event_attendees_v2 AS SELECT event_id, json_array_length(attendee_list) AS num_attendees FROM event_attendees WHERE json_array_length(attendee_list) > 0;

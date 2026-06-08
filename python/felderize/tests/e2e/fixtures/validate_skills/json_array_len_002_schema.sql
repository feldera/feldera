-- rule: json_array_len
-- spark: json_array_length(json_str) — length of a JSON array string
-- feldera: CARDINALITY(CAST(PARSE_JSON(json_str) AS VARIANT ARRAY))
CREATE TABLE event_attendees (event_id INT, attendee_list STRING);

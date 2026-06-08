-- rule: to_json_func
-- spark: to_json(v) — serialize a struct/map/array value to a JSON string
-- feldera: TO_JSON(v) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW event_json_v3 AS SELECT event_id, to_json(named_struct('name', event_name, 'code', event_code, 'id', event_id)) AS event_json FROM events_t3;

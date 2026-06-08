-- rule: lateral_view_posexplode
-- spark: LATERAL VIEW posexplode(arr) t AS pos, item — unnest with 0-based position index
-- feldera: UNNEST(arr) WITH ORDINALITY AS t(item, pos) — Feldera ordinal is 1-based, comes after value
CREATE OR REPLACE TEMP VIEW event_actions_with_sequence AS SELECT event_id, event_type, pos as action_sequence, item as action_name FROM event_log LATERAL VIEW posexplode(actions) t AS pos, item;

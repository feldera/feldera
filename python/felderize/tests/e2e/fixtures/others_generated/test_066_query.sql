CREATE OR REPLACE TEMP VIEW val08_upper_attrs AS
SELECT row_id, transform_keys(attrs, (k, v) -> upper(k)) AS upper_attrs
FROM collection_events;

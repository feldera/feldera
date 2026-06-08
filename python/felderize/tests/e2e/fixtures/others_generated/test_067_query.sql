CREATE OR REPLACE TEMP VIEW val09_lower_attrs AS
SELECT row_id, transform_values(attrs, (k, v) -> lower(v)) AS lower_attrs
FROM collection_events;

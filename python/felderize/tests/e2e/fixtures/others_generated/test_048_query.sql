CREATE OR REPLACE TEMP VIEW bm66_exploded_tags AS
SELECT event_id, tag FROM tagged_events LATERAL VIEW explode(tags) tag_table AS tag;

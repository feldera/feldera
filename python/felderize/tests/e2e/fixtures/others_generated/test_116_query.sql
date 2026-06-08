CREATE OR REPLACE TEMP VIEW val182_exists_number_threshold AS
SELECT row_id, exists(nums, x -> x > 100) AS has_large_num
FROM collection_events
WHERE size(nums) > 0;

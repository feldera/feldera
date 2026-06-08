CREATE OR REPLACE TEMP VIEW val181_transform_numbers_with_filter AS
SELECT row_id, transform(filter(nums, x -> x > 0), x -> x * 2) AS doubled_positive_nums
FROM collection_events;

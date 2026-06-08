-- rule: array_repeat
-- spark: array_repeat(val, n) — create array with val repeated n times
-- feldera: ARRAY_REPEAT(val, n)
CREATE OR REPLACE TEMP VIEW repeated_numbers_v3 AS SELECT id, num_value, array_repeat(num_value, repetitions) AS num_array FROM numbers_003;

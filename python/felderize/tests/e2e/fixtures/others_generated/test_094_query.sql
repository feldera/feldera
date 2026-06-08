CREATE OR REPLACE TEMP VIEW val103_chosen_name AS
SELECT row_id, nvl2(nullable_str, first_name, last_name) AS chosen_name
FROM scalar_function_rows
ORDER BY chosen_name DESC NULLS LAST, row_id ASC
LIMIT 10;

CREATE OR REPLACE TEMP VIEW val125_right_padded_code AS
SELECT row_id, rpad(code, 10, '_') AS right_padded_code
FROM scalar_function_rows
WHERE row_id IN (SELECT row_id FROM scalar_function_rows WHERE nullable_str IS NOT NULL);

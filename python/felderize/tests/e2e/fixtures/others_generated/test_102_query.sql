CREATE OR REPLACE TEMP VIEW val135_ends_with_alt AS
SELECT row_id, endswith(email, alt_str) AS ends_with_alt
FROM scalar_function_rows
WHERE row_id IN (SELECT row_id FROM scalar_function_rows WHERE nullable_str IS NOT NULL);

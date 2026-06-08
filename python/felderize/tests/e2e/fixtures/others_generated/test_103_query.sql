CREATE OR REPLACE TEMP VIEW val140_email_domain AS
SELECT row_id, split_part(email, '@', 2) AS email_domain
FROM scalar_function_rows
WHERE row_id IN (SELECT row_id FROM scalar_function_rows WHERE nullable_str IS NOT NULL);

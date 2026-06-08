CREATE OR REPLACE TEMP VIEW bm47_contact_normalization AS
SELECT customer_id, COALESCE(NULLIF(email, ''), NULLIF(backup_email, '')) AS primary_email FROM contact_records;

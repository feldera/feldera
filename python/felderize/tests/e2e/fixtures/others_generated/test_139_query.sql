CREATE OR REPLACE TEMP VIEW gpt139_overlay AS
SELECT
  doc_id,
  author,
  OVERLAY(content PLACING '***REDACTED***' FROM 1 FOR 10) AS redacted_content
FROM document_drafts;

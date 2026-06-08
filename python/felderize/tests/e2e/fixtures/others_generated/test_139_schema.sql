CREATE TABLE document_drafts (
  doc_id BIGINT,
  author STRING,
  content STRING
) USING parquet;

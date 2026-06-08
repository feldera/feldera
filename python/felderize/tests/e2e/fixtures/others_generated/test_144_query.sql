CREATE OR REPLACE TEMP VIEW gpt144_levenshtein AS
SELECT
  product_id,
  query_term,
  catalog_name,
  levenshtein(query_term, catalog_name) AS edit_distance
FROM product_search;

-- rule: transform_hof
-- spark: transform(arr, x -> expr) — apply lambda to each array element, return transformed array
-- feldera: TRANSFORM(arr, x -> expr) — same syntax, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW word_length_list_v3 AS SELECT doc_id, transform(words, w -> length(w)) AS lengths FROM word_lengths;

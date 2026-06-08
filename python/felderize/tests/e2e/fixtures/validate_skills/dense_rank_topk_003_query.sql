-- rule: dense_rank_topk
-- spark: DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) in TopK pattern — must be in subquery with WHERE filter
-- feldera: DENSE_RANK() same; wrap in subquery with WHERE dr <= N filter
CREATE OR REPLACE TEMP VIEW highest_rated_movies_v3 AS
SELECT genre, title, rating, release_year
FROM (
  SELECT genre, title, rating, release_year,
         DENSE_RANK() OVER (PARTITION BY genre ORDER BY rating DESC) AS dr
  FROM movie_ratings
)
WHERE dr <= 1;

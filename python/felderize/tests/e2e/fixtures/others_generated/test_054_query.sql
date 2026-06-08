CREATE OR REPLACE TEMP VIEW bm81_top_countries AS
SELECT DISTINCT country FROM country_events2 ORDER BY country ASC LIMIT 5;

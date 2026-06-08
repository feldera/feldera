CREATE OR REPLACE TEMP VIEW bm57_unique_visitors_estimate AS
SELECT site_id, approx_count_distinct(visitor_id) AS approx_unique_visitors FROM site_visits GROUP BY site_id;

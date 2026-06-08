CREATE OR REPLACE TEMP VIEW bm43_vip_large_spenders AS
SELECT * FROM (
  SELECT user_id, segment, SUM(amount) AS total_amount FROM spend_events GROUP BY user_id, segment
) s WHERE segment = 'VIP' AND total_amount > 10000;

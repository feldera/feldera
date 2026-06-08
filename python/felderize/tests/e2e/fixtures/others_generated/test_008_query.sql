CREATE OR REPLACE TEMP VIEW bm09_subscription_mix AS
SELECT
  plan_name,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(CASE WHEN is_trial THEN 1 ELSE 0 END) AS trial_subscriptions,
  SUM(CASE WHEN cancelled_at IS NULL THEN monthly_price ELSE 0 END) AS active_mrr
FROM subscriptions
GROUP BY plan_name;

CREATE OR REPLACE TEMP VIEW jn16_support_inner_join AS
SELECT
  c.region,
  a.team_name,
  COUNT(*) AS ticket_count
FROM support_tickets t
JOIN support_customers c
  ON t.customer_id = c.customer_id
JOIN support_agents a
  ON t.agent_id = a.agent_id
GROUP BY c.region, a.team_name;

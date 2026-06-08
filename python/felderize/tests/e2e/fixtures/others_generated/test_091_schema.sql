CREATE TABLE support_customers (
  customer_id BIGINT,
  region STRING,
  segment STRING
) USING parquet;

CREATE TABLE support_agents (
  agent_id BIGINT,
  team_name STRING,
  region STRING
) USING parquet;

CREATE TABLE support_tickets (
  ticket_id BIGINT,
  customer_id BIGINT,
  agent_id BIGINT,
  status STRING,
  priority STRING
) USING parquet;

CREATE TABLE support_escalations (
  escalation_id BIGINT,
  ticket_id BIGINT,
  level INT
) USING parquet;

CREATE TABLE support_surveys (
  ticket_id BIGINT,
  score INT
) USING parquet;

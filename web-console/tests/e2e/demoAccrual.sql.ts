export default `
-- slow changing TABLE
CREATE TABLE customer_t (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING NOT NULL
);

-- slow changing TABLE
CREATE TABLE workspace_t (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING NOT NULL,
    customer_id BIGINT NOT NULL FOREIGN KEY REFERENCES customer_t (id)
);

-- slow changing TABLE
CREATE TABLE work_t (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING NOT NULL,
    workspace_id BIGINT NOT NULL FOREIGN KEY REFERENCES workspace_t (id)
);

-- customer has only one credit tuple and it's non-changing
-- consumed tuples are fast changing and insert only
CREATE TABLE credit_t (
    id BIGINT NOT NULL PRIMARY KEY,
    total DOUBLE NOT NULL,
    customer_id BIGINT NOT NULL FOREIGN KEY REFERENCES customer_t (id)
);

-- slow changing TABLE
CREATE TABLE user_t (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING NOT NULL
);

-- fast changing TABLE
CREATE TABLE task_t (
    id BIGINT NOT NULL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    user_id BIGINT NOT NULL FOREIGN KEY REFERENCES user_t (id),
    work_id BIGINT NOT NULL FOREIGN KEY REFERENCES work_t (id),
    total DOUBLE NOT NULL
);

-- consumed total by work item.
CREATE VIEW work_consumed_v AS
SELECT work_id,
       SUM(total) as consumed
FROM task_t
GROUP BY work_id;

CREATE VIEW top10_users AS
SELECT user_id,
       SUM(total) as consumed
FROM task_t
    GROUP BY user_id
ORDER BY consumed DESC LIMIT 10;

-- consumed total by workspace.
CREATE VIEW workspace_consumed_v AS
SELECT DISTINCT
       customer_t.id as customer_id,
       workspace_t.id as workspace_id,
       SUM(work_consumed_v.consumed) OVER (PARTITION BY workspace_t.id ORDER BY customer_t.id) as consumed
FROM  work_consumed_v
      JOIN work_t ON work_t.id = work_consumed_v.work_id
      JOIN workspace_t ON work_t.workspace_id = workspace_t.id
      JOIN customer_t ON  workspace_t.customer_id = customer_t.id;

-- consumed total by customer.
CREATE VIEW customer_consumed_v AS
SELECT customer_id,
       SUM(consumed) as consumed
FROM  workspace_consumed_v
GROUP BY customer_id;

CREATE VIEW customer_total_credit_v AS
SELECT customer_t.id as customer_id,
       SUM(credit_t.total) as total
FROM customer_t
     LEFT JOIN credit_t ON credit_t.customer_id = customer_t.id
GROUP BY customer_t.id;

CREATE VIEW customer_balance_v as
SELECT
       coalesce(customer_consumed_v.customer_id, customer_total_credit_v.customer_id) as customer_id,
       customer_total_credit_v.total as total_credit,
       customer_consumed_v.consumed as total_consumed,
       (coalesce(customer_total_credit_v.total,0) - coalesce(customer_consumed_v.consumed, 0)) as balance
FROM customer_consumed_v
     FULL JOIN customer_total_credit_v
     ON customer_consumed_v.customer_id = customer_total_credit_v.customer_id;
`

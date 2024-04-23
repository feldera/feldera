from sqlglot import parse, parse_one
from sqlglot.optimizer import optimize
from sqlglot.planner import Plan
import sqlglot


def to_ast(sql):
    node = parse_one(sql)
    optimized = optimize(node, leave_tables_isolated=True)
    print(optimized.sql(pretty=True))
    return optimized


def to_plan(sql):
    return Plan(to_ast(sql))


if __name__ == "__main__":
    # op2 = to_plan("CREATE VIEW output_users AS SELECT * FROM users;")
    # op3 = to_plan("SELECT * FROM output_users;")
    # print(execute("CREATE TABLE users (name varchar)"))

    tables = {
        "sushi": [
            {"id": 1, "price": 1.0},
            {"id": 2, "price": 2.0},
            {"id": 3, "price": 3.0},
        ],
        "order_items": [
            {"sushi_id": 1, "order_id": 1},
            {"sushi_id": 1, "order_id": 1},
            {"sushi_id": 2, "order_id": 1},
            {"sushi_id": 3, "order_id": 2},
        ],
        "orders": [
            {"id": 1, "user_id": 1},
            {"id": 2, "user_id": 2},
        ],
    }
    sql2 = """SELECT
  o.user_id,
  SUM(s.price) AS price
FROM orders o
JOIN order_items i
  ON o.id = i.order_id
JOIN sushi s
  ON i.sushi_id = s.id
GROUP BY o.user_id
"""

    sql = """SELECT * FROM orders"""

    from sqlglot import executor as pythonexecutor

    r = pythonexecutor.execute(
        sql,
        tables=tables
    )
    print(r)

    import executor as dbspexecutor

    r = dbspexecutor.execute(
        sql,
        tables=tables
    )
    print(r)

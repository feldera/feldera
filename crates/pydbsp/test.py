from sqlglot import parse, parse_one
from sqlglot.optimizer import optimize


def to_ast(sql):
    node = parse_one(sql)
    optimized = optimize(node, leave_tables_isolated=True)
    print(optimized.sql(pretty=True))
    return optimized


if __name__ == "__main__":
    # op1 = to_ast("CREATE TABLE users (name varchar);")
    # op2 = to_ast("CREATE VIEW output_users AS SELECT * FROM users;")
    import pydbsp

    r = pydbsp.Runtime(3)

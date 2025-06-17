from itertools import product
from tests.aggregate_tests.aggtst_base import TstView


# list of table names where column c1 gets cast to varchar
convert_vchar_tbl = [
    "orderby_tbl_sqlite_decimal_char",
    "orderby_tbl_sqlite_real_date",
    "orderby_tbl_sqlite_double_date",
]


def create_orderby_clause(col: str, direction: str, nulls: str):
    """Attach the ORDER BY fragment for each column in the view query"""
    clause = f"{col} {direction}"
    if nulls:
        clause += f" NULLS {nulls}"
    return clause


class AutomateOrderByTests:
    """Generate views with all possible combinations of ORDER BY clause for columns c1 and c2
    Based on the values of direction and nulls for each column"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.views = []
        self.generate_orderby_views()

    def generate_orderby_views(self):
        """Generate 36 unique ORDER BY view combinations for each table with two columns, named c1 and c2"""
        directions = ["ASC", "DESC"]
        nulls_options = ["", "FIRST", "LAST"]
        count = 1

        for c1_dir, c1_nulls, c2_dir, c2_nulls in product(
            directions, nulls_options, directions, nulls_options
        ):
            # Fill direction and null values by iterating over all possible input values for columns c1 and c2
            order_clause = ", ".join(
                [
                    create_orderby_clause("c1", c1_dir, c1_nulls),
                    create_orderby_clause("c2", c2_dir, c2_nulls),
                ]
            )
            # Assign names to views based on the table name and the value of the counter
            view_name = f"view_{self.table_name}{count}"
            if self.table_name in convert_vchar_tbl:
                sql1 = "CAST(c1 AS VARCHAR) AS c1, c2"
            else:
                sql1 = "*"
            sql = f"""CREATE MATERIALIZED VIEW {view_name} AS
                          SELECT
                          {sql1}
                          FROM {self.table_name}
                          ORDER BY {order_clause}
                          LIMIT 3"""
            self.views.append((view_name, sql))
            count += 1

    def register(self, ta):
        """Register views with the Test Accumulator
        Named `register` because `register_tests_in_module` calls: `instance.register(ta)`"""
        for view_name, sql in self.views:
            v = TstView()
            v.sql = sql
            v.data = []  # SQLite will populate this later
            v.register(ta)

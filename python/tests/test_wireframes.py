import unittest
import pandas as pd

from feldera import SQLContext, SQLSchema
from tests import TEST_CLIENT


class TestWireframes(unittest.TestCase):
    def test_local(self):
        sql = SQLContext('notebook', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        view_name = "average_scores"

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.register_table(TBL_NAMES[0], SQLSchema({"name": "STRING", "id": "INT"}))
        sql.register_table(TBL_NAMES[1], SQLSchema({
            "student_id": "INT",
            "science": "INT",
            "maths": "INT",
            "art": "INT"
        }))

        query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC"
        sql.register_view(view_name, query)

        sql.connect_source_pandas(TBL_NAMES[0], df_students)
        sql.connect_source_pandas(TBL_NAMES[1], df_grades)

        out = sql.listen(view_name)

        sql.run_to_completion()

        df = out.to_pandas()

        assert df.shape[0] == 100

    def test_two_SQLContexts(self):
        # https://github.com/feldera/feldera/issues/1770
        
        sql = SQLContext('sql_context1', TEST_CLIENT).get_or_create()
        sql2 = SQLContext('sql_context2', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        VIEW_NAMES = [n + "_view" for n in TBL_NAMES]

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.register_table(TBL_NAMES[0], SQLSchema({"name": "STRING", "id": "INT"}))
        sql2.register_table(TBL_NAMES[1], SQLSchema({
            "student_id": "INT",
            "science": "INT",
            "maths": "INT",
            "art": "INT"
        }))

        sql.register_view(VIEW_NAMES[0], f"SELECT * FROM {TBL_NAMES[0]}")
        sql2.register_view(VIEW_NAMES[1], f"SELECT * FROM {TBL_NAMES[1]}")

        sql.connect_source_pandas(TBL_NAMES[0], df_students)
        sql2.connect_source_pandas(TBL_NAMES[1], df_grades)

        out = sql.listen(VIEW_NAMES[0])
        out2 = sql2.listen(VIEW_NAMES[1])

        sql.run_to_completion()
        sql2.run_to_completion()

        df = out.to_pandas()
        df2 = out2.to_pandas()

        assert df.columns.tolist() not in df2.columns.tolist()

    def test_foreach_chunk(self):
        def callback(df: pd.DataFrame, seq_no: int):
            print(f"\nSeq No: {seq_no}, DF size: {df.shape[0]}\n")

        sql = SQLContext('foreach_chunk', TEST_CLIENT).get_or_create()

        TBL_NAMES = ['students', 'grades']
        view_name = "average_scores"

        df_students = pd.read_csv('students.csv')
        df_grades = pd.read_csv('grades.csv')

        sql.register_table(TBL_NAMES[0], SQLSchema({"name": "STRING", "id": "INT"}))
        sql.register_table(TBL_NAMES[1], SQLSchema({
            "student_id": "INT",
            "science": "INT",
            "maths": "INT",
            "art": "INT"
        }))

        query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC"
        sql.register_view(view_name, query)

        sql.connect_source_pandas(TBL_NAMES[0], df_students)
        sql.connect_source_pandas(TBL_NAMES[1], df_grades)

        sql.foreach_chunk(view_name, callback)

        sql.run_to_completion()

    def test_df_without_columns(self):

        sql = SQLContext('df_without_columns', TEST_CLIENT).get_or_create()
        TBL_NAME = "student"

        df = pd.DataFrame([(1, "a"), (2, "b"), (3, "c")])

        sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING"}))
        sql.register_view("s", f"SELECT * FROM {TBL_NAME}")

        with self.assertRaises(ValueError):
            sql.connect_source_pandas(TBL_NAME, df)


if __name__ == '__main__':
    unittest.main()

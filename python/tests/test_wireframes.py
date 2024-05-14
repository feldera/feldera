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

        sql.input_from_pandas(TBL_NAMES[0], df_students)
        sql.input_from_pandas(TBL_NAMES[1], df_grades)

        out = sql.listen(view_name)

        sql.run_to_completion()

        df = out.to_pandas()
        print()
        print(df)

        sql.shutdown()


if __name__ == '__main__':
    unittest.main()

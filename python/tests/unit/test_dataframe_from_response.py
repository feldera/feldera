import unittest

from feldera._helpers import dataframe_from_response


def nullable_field(name: str, sql_type: str) -> dict:
    return {
        "name": name,
        "case_sensitive": False,
        "columntype": {"type": sql_type, "nullable": True},
    }


class TestDataframeFromResponse(unittest.TestCase):
    def test_empty_batches_keep_schema(self):
        fields = [
            nullable_field("ID", "INTEGER"),
            nullable_field("S", "VARCHAR"),
            {**nullable_field("Quoted", "BOOLEAN"), "case_sensitive": True},
        ]
        for buffer in [[], [[]], [[], []]]:
            with self.subTest(buffer=buffer):
                df = dataframe_from_response(buffer, fields)

                self.assertTrue(df.empty)
                self.assertEqual(list(df.columns), ["id", "s", "Quoted", "insert_delete"])
                self.assertEqual(str(df["id"].dtype), "Int32")
                self.assertEqual(str(df["s"].dtype), "string")
                self.assertEqual(str(df["Quoted"].dtype), "boolean")

    def test_null_strings_stay_null(self):
        fields = [
            nullable_field("id", "INTEGER"),
            nullable_field("s", "VARCHAR"),
            nullable_field("c", "CHAR"),
        ]
        changes = [
            {"insert": {"id": 1, "s": None, "c": None}},
            {"insert": {"id": 2, "s": "None", "c": "None"}},
            {"insert": {"id": 3, "s": "a", "c": "a"}},
        ]

        df = dataframe_from_response([changes], fields)

        assert df["s"].isna().tolist() == [True, False, False]
        assert df["c"].isna().tolist() == [True, False, False]
        assert df.to_dict(orient="records") == [
            {"id": 1, "s": None, "c": None, "insert_delete": 1},
            {"id": 2, "s": "None", "c": "None", "insert_delete": 1},
            {"id": 3, "s": "a", "c": "a", "insert_delete": 1},
        ]


if __name__ == "__main__":
    unittest.main()

import unittest

from feldera import PipelineBuilder
from tests import TEST_CLIENT
from decimal import Decimal


class TestVariant(unittest.TestCase):
    def test_local(self):
        sql = """
-- Ingest JSON as string; output it as VARIANT.
CREATE TABLE json_table (json VARCHAR) with ('materialized' = 'true');
CREATE MATERIALIZED VIEW json_view AS SELECT PARSE_JSON(json) AS json FROM json_table;
CREATE MATERIALIZED VIEW json_string_view AS SELECT TO_JSON(json) AS json FROM json_view;

CREATE MATERIALIZED VIEW average_view AS SELECT
CAST(json['name'] AS VARCHAR) as name,
((CAST(json['scores'][1] AS DECIMAL(8, 2)) + CAST(json['scores'][2] AS DECIMAL(8, 2))) / 2) as average
FROM json_view;

-- Ingest JSON as variant; extract strongly typed columns from it.
CREATE TABLE variant_table(val VARIANT) with ('materialized' = 'true');

CREATE MATERIALIZED VIEW typed_view AS SELECT
    CAST(val['name'] AS VARCHAR) as name,
    CAST(val['scores'] AS DECIMAL ARRAY) as scores
FROM variant_table;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_variant", sql=sql
        ).create_or_replace()

        input_strings = [
            {"json": '{"name":"Bob","scores":[8,10]}'},
            {"json": '{"name":"Dunce","scores":[3,4]}'},
            {"json": '{"name":"John","scores":[9,10]}'},
        ]

        input_json = [
            {"val": {"name": "Bob", "scores": [8, 10]}},
            {"val": {"name": "Dunce", "scores": [3, 4]}},
            {"val": {"name": "John", "scores": [9, 10]}},
        ]

        expected_strings = [j | {"insert_delete": 1} for j in input_strings]

        expected_average = [
            {"name": "Bob", "average": Decimal(9)},
            {"name": "Dunce", "average": Decimal(3.5)},
            {"name": "John", "average": Decimal(9.5)},
        ]
        for datum in expected_average:
            datum.update({"insert_delete": 1})

        expected_typed = [
            {"name": "Bob", "scores": [8, 10]},
            {"name": "Dunce", "scores": [3, 4]},
            {"name": "John", "scores": [9, 10]},
        ]
        for datum in expected_typed:
            datum.update({"insert_delete": 1})

        expected_variant = [
            {"json": {"name": "Bob", "scores": [8, 10]}},
            {"json": {"name": "Dunce", "scores": [3, 4]}},
            {"json": {"name": "John", "scores": [9, 10]}},
        ]
        for datum in expected_variant:
            datum.update({"insert_delete": 1})

        variant_out = pipeline.listen("json_view")
        json_out = pipeline.listen("json_string_view")
        average_out = pipeline.listen("average_view")
        typed_out = pipeline.listen("typed_view")

        pipeline.start()

        # Feed JSON as strings, receive output from `average_view` and `json_view`
        pipeline.input_json("json_table", input_strings)

        pipeline.wait_for_completion(False)

        assert expected_average == average_out.to_dict()
        assert expected_variant == variant_out.to_dict()

        assert expected_strings == json_out.to_dict()

        # Feed VARIANT, read strongly typed columns. Since output colums have the same
        # shape as inputs, output and input should be identical.
        pipeline.input_json("variant_table", input_json)
        pipeline.wait_for_completion(False)

        assert expected_typed == typed_out.to_dict()

        pipeline.wait_for_completion(True)

        pipeline.delete()


if __name__ == "__main__":
    unittest.main()

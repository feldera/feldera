import os
import unittest
import pandas as pd

import unittest
from feldera import PipelineBuilder, Pipeline
from tests import TEST_CLIENT
from decimal import Decimal

class TestVariant(unittest.TestCase):
    def test_local(self):
        sql = f"""
CREATE TABLE json_table (json VARCHAR);
CREATE TABLE variant_table(val VARIANT);

CREATE LOCAL VIEW tmp AS SELECT PARSE_JSON(json) AS json FROM json_table;

CREATE VIEW average AS SELECT
CAST(json['name'] AS VARCHAR) as name,
((CAST(json['scores'][1] AS DECIMAL(8, 2)) + CAST(json['scores'][2] AS DECIMAL(8, 2))) / 2) as average
FROM tmp;

CREATE VIEW typed_view AS SELECT
    CAST(val['name'] AS VARCHAR) as name,
    CAST(val['scores'] AS DECIMAL ARRAY) as scores
FROM variant_table;


        """

        dir_path = os.path.dirname(os.path.realpath(__file__))
        pipeline = PipelineBuilder(TEST_CLIENT, name="test_variant", sql=sql).create_or_replace()
        df_grades = pd.read_csv(dir_path + '/grades-json.csv')

        expected_average = [
            { "name": "Bob", "average": Decimal(9) },
            { "name": "Dunce", "average": Decimal(3.5) },
            { "name": "John", "average": Decimal(9.5) }
        ]
        for datum in expected_average:
            datum.update({"insert_delete": 1})

        average_out = pipeline.listen("average")
        typed_out = pipeline.listen("typed_view")

        pipeline.start()

        pipeline.input_pandas('json_table', df_grades)
        pipeline.wait_for_completion(True)
        out_data = average_out.to_dict()

        assert expected_average == out_data

        pipeline.input_pandas('variant_table', df_grades)
        pipeline.wait_for_completion(True)
        out_data = typed_out.to_dict()

        expected_typed = [
            { "name": "Bob", "average": [Decimal(8), Decimal(10)] },
            { "name": "Dunce", "average": [Decimal(9), Decimal(10)] },
            { "name": "John", "average": [Decimal(3), Decimal(4)] }
        ]
        for datum in expected_average:
            datum.update({"insert_delete": 1})

        assert expected_typed == out_data

        pipeline.delete()

if __name__ == '__main__':
    unittest.main()

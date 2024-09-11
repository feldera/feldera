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
CREATE TABLE json (json VARCHAR);

CREATE LOCAL VIEW tmp AS SELECT PARSE_JSON(json) AS json FROM json;

CREATE VIEW average AS SELECT
CAST(json['name'] AS VARCHAR) as name,
((CAST(json['scores'][1] AS DECIMAL(8, 2)) + CAST(json['scores'][2] AS DECIMAL(8, 2))) / 2) as average
FROM tmp;
        """

        dir_path = os.path.dirname(os.path.realpath(__file__))
        pipeline = PipelineBuilder(TEST_CLIENT, name="test_variant", sql=sql).create_or_replace()
        df_grades = pd.read_csv(dir_path + '/grades-json.csv')

        expected_data = [
            { "name": "Bob", "average": Decimal(9) },
            { "name": "Dunce", "average": Decimal(3.5) },
            { "name": "John", "average": Decimal(9.5) }
        ]
        for datum in expected_data:
            datum.update({"insert_delete": 1})

        out = pipeline.listen("average")

        pipeline.start()
        pipeline.input_pandas('json', df_grades)
        pipeline.wait_for_completion(True)
        out_data = out.to_dict()

        assert expected_data == out_data
        pipeline.delete()

if __name__ == '__main__':
    unittest.main()

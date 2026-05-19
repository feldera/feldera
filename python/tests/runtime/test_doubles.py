import unittest

import math
from feldera import PipelineBuilder
from tests import TEST_CLIENT
from tests.platform.helper import PipelineTestCase
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS


# Test serialization of doubles
class TestDouble(PipelineTestCase):
    def test_local(self):
        sql = """
CREATE TABLE t (
    d DOUBLE
) WITH ('connectors' = '[{
   "name": "t",
   "transport": {
      "name": "datagen",
      "config": {
         "seed": 1,
         "plan": [{
            "limit": 1,
            "fields": {
               "d": { "values": [0] }
            }
         }]
      }
   }
}]');

CREATE MATERIALIZED VIEW v AS
SELECT 1/d as one, 0/d as zero FROM t;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=self.register_for_cleanup("test_doubles"),
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()

        pipeline.start_paused()
        pipeline.resume()

        pipeline.wait_for_completion()
        data = list(pipeline.query_arrow_dicts("SELECT * FROM v"))
        assert len(data) == 1
        assert data[0]["one"] == float("inf")
        assert math.isnan(data[0]["zero"])
        pipeline.stop(force=True)
        pipeline.delete(True)


if __name__ == "__main__":
    unittest.main()

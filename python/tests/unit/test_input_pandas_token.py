import unittest
from types import SimpleNamespace

import pandas as pd

from feldera.pipeline import Pipeline


class FakeClient:
    """Answers the status and schema lookups, and returns a new token for each push."""

    def __init__(self):
        self.pushes = 0

    def get_pipeline(self, pipeline_name, field_selector):
        return SimpleNamespace(
            name=pipeline_name,
            deployment_status="Running",
            tables=[SimpleNamespace(name="t")],
        )

    def push_to_pipeline(self, *args, **kwargs):
        self.pushes += 1
        return f"token-{self.pushes}"


class TestInputPandasToken(unittest.TestCase):
    def pipeline(self) -> Pipeline:
        pipeline = Pipeline(FakeClient())
        pipeline._inner = SimpleNamespace(name="p")
        return pipeline

    def test_returns_token_of_last_chunk(self):
        pipeline = self.pipeline()
        # 2,500 rows are pushed in three chunks of at most 1,000 rows.
        df = pd.DataFrame({"x": range(2_500)})

        token = pipeline.input_pandas("t", df)

        assert pipeline.client.pushes == 3
        assert token == "token-3"

    def test_empty_dataframe_returns_none(self):
        pipeline = self.pipeline()

        token = pipeline.input_pandas("t", pd.DataFrame({"x": []}))

        assert pipeline.client.pushes == 0
        assert token is None


if __name__ == "__main__":
    unittest.main()

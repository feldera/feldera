import unittest

from tests.shared_test_pipeline import SharedTestPipeline


class TestPipeline(SharedTestPipeline):
    def test_create_pipeline(self):
        """
        CREATE TABLE tbl(id INT) WITH ('materialized' = 'true');
        CREATE MATERIALIZED VIEW v0 AS SELECT * FROM tbl;
        """
        pass

    def test_get_pipeline_logs(self):
        self.pipeline.start()
        # regression test for https://github.com/feldera/feldera/issues/4394
        for _ in range(200):
            logs = self.pipeline.logs()
            start = next(logs)
            assert "Fresh start of pipeline log" in start
        self.pipeline.pause()
        self.pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()

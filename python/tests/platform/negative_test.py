import unittest
from feldera import PipelineBuilder, Pipeline
from feldera.testutils import unique_pipeline_name
from tests import TEST_CLIENT


class NegativeCompilationTests(unittest.TestCase):
    def test_sql_error(self):
        pipeline_name = unique_pipeline_name("sql_error")
        sql = """
CREATE TABLE student(
    id INT,
    name STRING
);
CREATE VIEW s AS SELECT * FROM blah;
        """
        expected = f"""
Pipeline {pipeline_name} failed to compile:
Compilation error
From line 5, column 32 to line 5, column 35: Object 'blah' not found
Code snippet:
    5|CREATE VIEW s AS SELECT * FROM blah;
                                     ^^^^""".strip()
        with self.assertRaises(Exception) as err:
            PipelineBuilder(
                TEST_CLIENT, name=pipeline_name, sql=sql
            ).create_or_replace()
        got_err: str = err.exception.args[0].strip()
        assert expected == got_err
        pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
        pipeline.clear_storage()

    def test_rust_error(self):
        pipeline_name = unique_pipeline_name("rust_error")
        sql = ""

        with self.assertRaises(Exception) as err:
            PipelineBuilder(
                TEST_CLIENT, name=pipeline_name, sql=sql, udf_rust="Davy Jones"
            ).create_or_replace()

        assert "Davy Jones" in err.exception.args[0].strip()

    def test_program_error0(self):
        sql = "create taabl;"
        name = unique_pipeline_name("test_program_error0")
        try:
            _ = PipelineBuilder(TEST_CLIENT, name, sql).create_or_replace()
        except Exception:
            pass
        pipeline = Pipeline.get(name, TEST_CLIENT)
        err = pipeline.program_error()
        assert err["sql_compilation"] != 0
        pipeline.stop(force=True)
        pipeline.clear_storage()

    def test_program_error1(self):
        sql = ""
        name = unique_pipeline_name("test_program_error1")
        _ = PipelineBuilder(TEST_CLIENT, name, sql).create_or_replace()
        pipeline = Pipeline.get(name, TEST_CLIENT)
        err = pipeline.program_error()
        assert err["sql_compilation"]["exit_code"] == 0
        assert err["rust_compilation"]["exit_code"] == 0
        pipeline.stop(force=True)
        pipeline.clear_storage()

    def test_errors0(self):
        sql = "SELECT invalid"
        name = unique_pipeline_name("test_errors0")
        try:
            _ = PipelineBuilder(TEST_CLIENT, name, sql).create_or_replace()
        except Exception:
            pass
        pipeline = Pipeline.get(name, TEST_CLIENT)
        assert pipeline.errors()[0]["sql_compilation"]["exit_code"] != 0

    def test_initialization_error(self):
        sql = """
        CREATE TABLE t0 (
            c0 INT NOT NULL
        ) with (
          'connectors' = '[{
            "transport": {
              "name": "datagen",
              "config": {
                "plan": [{
                    "fields": {
                        "c1": { "strategy": "uniform", "range": [100, 10000] }
                    }
                }]
              }
            }
          }]'
        );
        """
        pipeline = PipelineBuilder(
            TEST_CLIENT, name=unique_pipeline_name("test_initialization_error"), sql=sql
        ).create_or_replace()
        with self.assertRaises(RuntimeError) as err:
            pipeline.start()
        pipeline.stop(force=True)
        pipeline.clear_storage()
        got_err: str = err.exception.args[0].strip()
        assert "Unable to START the pipeline" in got_err


if __name__ == "__main__":
    unittest.main()

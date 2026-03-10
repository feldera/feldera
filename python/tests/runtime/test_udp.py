import unittest

from feldera import PipelineBuilder
from tests import TEST_CLIENT, unique_pipeline_name
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS


# Test user-defined preprocessor
class TestUDP(unittest.TestCase):
    def test_local(self):
        sql = """
CREATE TABLE t (
    i INT,
    ti TINYINT,
    si SMALLINT,
    bi BIGINT,
    r REAL,
    d DOUBLE,
    bin VARBINARY,
    dt DATE,
    t TIME,
    ts TIMESTAMP,
    a INT ARRAY,
    b BOOLEAN,
    dc DECIMAL(7,2),
    s VARCHAR
) WITH ('connectors' = '[{
   "name": "t",
   "transport": {
      "name": "datagen",
      "config": {
         "seed": 1,
         "plan": [{
            "limit": 100000
         }]
      }
   },
   "preprocessor": [{
      "name": "logger",
      "message_oriented": true,
      "config": {}
   }]
}]');

CREATE MATERIALIZED VIEW v AS
SELECT * FROM t;
        """

        udfs = """
use tracing::info;
use std::sync::{Arc, Mutex};
use feldera_adapterlib::format::{ParseError, Splitter};
use feldera_adapterlib::preprocess::{
    Preprocessor, PreprocessorCreateError, PreprocessorFactory,
};
use feldera_types::preprocess::PreprocessorConfig;

pub struct LoggerPreprocessor {
   count: Arc<Mutex<u64>>,
}

impl Preprocessor for LoggerPreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        let mut count = self.count.lock().unwrap();
        *count += data.len() as u64;
        // Log a message if the counter has crossed a Megabyte boundary
        if *count / (1024 * 1024) > (*count - data.len() as u64) / (1024 * 1024) {
            info!("Processed {} bytes of data", *count);
        }
        (data.to_vec(), vec![])
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(LoggerPreprocessor { count: Arc::clone(&self.count) })
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        None
    }
}

pub struct LoggerPreprocessorFactory;

impl PreprocessorFactory for LoggerPreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(LoggerPreprocessor { count: Arc::new(Mutex::new(0)) }))
    }
}
"""

        toml = """
tracing = { version = "0.1.40" }
"""

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=unique_pipeline_name("test_udps"),
            sql=sql,
            udf_rust=udfs,
            udf_toml=toml,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()

        pipeline.start_paused()
        pipeline.resume()

        pipeline.wait_for_completion()
        for log in pipeline.logs():
           # This will loop forever if the message is not found
           if "bytes of data" in log:
               break
        hash = pipeline.query_hash("SELECT * FROM v ORDER BY i, ti, si, bi")
        assert (
            hash == "0F5CD4C02B4670AB14FE753523D7D9962E251850D8AD247EC04ABC1531EB4AF3"
        ), "Hash does not match"
        pipeline.stop(force=True)
        pipeline.delete(True)


if __name__ == "__main__":
    unittest.main()

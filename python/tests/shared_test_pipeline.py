import logging
import unittest

from feldera import Pipeline, PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    reclaim_pipeline,
    unique_pipeline_name,
)
from tests import TEST_CLIENT

logger = logging.getLogger(__name__)


def sql(text_or_iterable):
    """
    Decorator to attach SQL (string or list/tuple of strings) to a test method.
    """

    def _wrap(fn):
        fn.SQL = text_or_iterable
        return fn

    return _wrap


class SharedTestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._ddls = []
        cls.client = TEST_CLIENT
        cls.pipeline_name = unique_pipeline_name(cls.__name__)
        for attr in dir(cls):
            if not attr.startswith("test_"):
                continue

            func = getattr(cls, attr)
            # Check for enterprise_only decorator
            is_enterprise_only = getattr(func, "_enterprise_only", False)
            if (
                is_enterprise_only
                and not cls.client.get_config().edition.is_enterprise()
            ):
                continue  # Skip DDL for enterprise-only tests if not enterprise

            ddl = getattr(func, "SQL", getattr(func, "__doc__", None))
            if ddl and ddl.strip() not in cls._ddls:
                cls._ddls.append(ddl.strip())

        if not hasattr(cls, "_pipeline"):
            cls.ddl = "\n".join(cls._ddls)
            cls._pipeline = PipelineBuilder(
                cls.client,
                cls.pipeline_name,
                cls.ddl,
                runtime_config=RuntimeConfig(
                    workers=FELDERA_TEST_NUM_WORKERS,
                    hosts=FELDERA_TEST_NUM_HOSTS,
                    logging="debug",
                ),
            ).create_or_replace()

    def setUp(self):
        self._owned_pipelines = []
        self.p = self._build_pipeline(self._testMethodName)

    def tearDown(self):
        """Force-stop and clear every pipeline the test owns."""
        for pipeline in self._owned_pipelines:
            for failure in reclaim_pipeline(pipeline.name):
                logger.warning("pipeline teardown: %s", failure)

    @property
    def pipeline(self) -> Pipeline:
        return self.p

    def new_pipeline_with_suffix(self, suffix: str) -> Pipeline:
        return self._build_pipeline(f"{self._testMethodName}_{suffix}")

    def _build_pipeline(self, base_name: str) -> Pipeline:
        """Create a pipeline over the class DDL and hand it to `tearDown`."""
        pipeline = PipelineBuilder(
            self.client,
            unique_pipeline_name(base_name),
            sql=self.ddl,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                logging="debug",
            ),
        ).create_or_replace()
        self._owned_pipelines.append(pipeline)
        return pipeline

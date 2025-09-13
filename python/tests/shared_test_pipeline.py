import unittest
from feldera.testutils import unique_pipeline_name
from tests import TEST_CLIENT
from feldera import PipelineBuilder, Pipeline


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
            ).create_or_replace()

    def setUp(self):
        p = PipelineBuilder(
            self.client, self._testMethodName, sql=self.ddl
        ).create_or_replace()
        self.p = p

    def tearDown(self):
        self.p.stop(force=True)
        self.p.clear_storage()

    @property
    def pipeline(self) -> Pipeline:
        return self.p

    def new_pipeline_with_suffix(self, suffix: str) -> Pipeline:
        return PipelineBuilder(
            self.client, unique_pipeline_name(f"{self._testMethodName}_{suffix}"), sql=self.ddl
        ).create_or_replace()

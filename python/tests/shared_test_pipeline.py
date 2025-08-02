import unittest
from tests import TEST_CLIENT
from feldera import PipelineBuilder, Pipeline


class SharedTestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._ddls = []
        cls.client = TEST_CLIENT
        cls.pipeline_name = cls.__name__
        for attr in dir(cls):
            if attr.startswith("test_"):
                func = getattr(cls, attr)
                ddl = getattr(func, "__doc__", None)
                # Check for enterprise_only decorator
                is_enterprise_only = getattr(func, "_enterprise_only", False)
                if (
                    is_enterprise_only
                    and not TEST_CLIENT.get_config().edition.is_enterprise()
                ):
                    continue  # Skip DDL for enterprise-only tests if not enterprise
                if ddl:
                    if ddl not in cls._ddls:
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

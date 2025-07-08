import unittest
from tests import TEST_CLIENT
from feldera import PipelineBuilder, Pipeline
from feldera.runtime_config import RuntimeConfig
from feldera.enums import PipelineStatus


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
                    if not ddl in cls._ddls:
                        cls._ddls.append(ddl.strip())

        if not hasattr(cls, "_pipeline"):
            cls.ddl = "\n".join(cls._ddls)
            cls._pipeline = PipelineBuilder(
                cls.client,
                cls.pipeline_name,
                cls.ddl,
            ).create_or_replace()

    def set_runtime_config(self, runtime_config: RuntimeConfig):
        self._pipeline = PipelineBuilder(
            self.client, self.pipeline_name, self.ddl, runtime_config=runtime_config
        ).create_or_replace()

    def reset_runtime_config(self):
        self.set_runtime_config(RuntimeConfig.default())

    @classmethod
    def tearDownClass(cls):
        if cls._pipeline is not None:
            if cls._pipeline.status() != PipelineStatus.STOPPED:
                cls._pipeline.stop(force=True)
            cls._pipeline.clear_storage()
            cls._pipeline = None
            cls._ddls = []

    @property
    def pipeline(self) -> Pipeline:
        p = type(self)._pipeline
        assert p is not None, "Shared pipeline was not created successfully."
        return p

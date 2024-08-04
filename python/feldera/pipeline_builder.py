from typing import Self

from feldera.rest.feldera_client import FelderaClient
from feldera.rest.pipeline import Pipeline as InnerPipeline
from feldera.pipeline import Pipeline
from feldera.enums import CompilationProfile
from feldera.runtime_config import RuntimeConfig, Resources
from feldera.rest.errors import FelderaAPIError


class PipelineBuilder:
    """
    A builder for creating a Pipeline in Feldera.

    :param client: The `.FelderaClient` instance
    :param name: The name of the pipeline
    :param description: The description of the pipeline
    :param sql: The SQL code of the pipeline
    :param compilation_profile: The compilation profile to use
    :param runtime_config: The runtime config to use
    """

    def __init__(
        self,
        client: FelderaClient,
        name: str = None,
        sql: str = "",
        description: str = "",
        compilation_profile: CompilationProfile = CompilationProfile.OPTIMIZED,
        runtime_config: RuntimeConfig = RuntimeConfig(resources=Resources()),

    ):
        self.client: FelderaClient = client
        self.name: str | None = name
        self.description: str = description
        self.sql: str = sql
        self.compilation_profile: CompilationProfile = compilation_profile
        self.runtime_config: RuntimeConfig = runtime_config

    def with_sql(self, sql: str) -> Self:
        """
        Sets the SQL code of the pipeline.

        :param sql: The SQL code of the pipeline
        """

        self.sql = sql
        return self

    def with_name(self, name: str) -> Self:
        """
        Sets the name of the pipeline.

        :param name: The name of the pipeline
        """

        self.name = name
        return self

    def with_description(self, description: str) -> Self:
        """
        Sets the description of the pipeline.

        :param description: The description of the pipeline
        """

        self.description = description
        return self

    def with_compilation_profile(self, compilation_profile: CompilationProfile) -> Self:
        """
        Sets the compilation profile of the pipeline.

        :param compilation_profile: The compilation profile to use
        """

        self.compilation_profile = compilation_profile
        return self

    def with_runtime_config(self, config: RuntimeConfig) -> Self:
        """
        Sets the runtime config of the pipeline.

        :param config: The runtime config to use
        """

        self.runtime_config = config
        return self

    def create(self) -> Pipeline:
        """
        Create the pipeline if it does not exist.

        :return: The created pipeline
        """

        inner = InnerPipeline(
            self.name,
            description=self.description,
            sql=self.sql,
            program_config={
                'profile': self.compilation_profile.value,
            },
            runtime_config=self.runtime_config.__dict__,
        )

        inner = self.client.create_pipeline(inner)
        pipeline = Pipeline(inner.name, self.client)
        pipeline._inner = inner

        return pipeline

    def get(self) -> Pipeline:
        """
        Get the pipeline if it exists.
        """

        try:
            inner = self.client.get_pipeline(self.name)
            pipeline = Pipeline(inner.name, self.client)
            pipeline.__inner = inner
            return pipeline
        except FelderaAPIError as err:
            if err.status_code == 404:
                raise RuntimeError(f"Pipeline with name {self.name} not found")

    def create_or_replace(self) -> Pipeline:
        """
        Creates a pipeline if it does not exist and replaces it if it exists.

        If the pipeline exists and is running, it will be stopped and replaced.
        """

        try:
            # shutdown the pipeline if it exists and is running
            self.client.shutdown_pipeline(self.name)
        except FelderaAPIError:
            # pipeline doesn't exist, no worries
            pass

        inner = InnerPipeline(
            self.name,
            description=self.description,
            sql=self.sql,
            program_config={
                'profile': self.compilation_profile.value,
            },
            runtime_config=self.runtime_config.__dict__,
        )

        inner = self.client.create_or_update_pipeline(inner)
        pipeline = Pipeline(inner.name, self.client)
        pipeline._inner = inner

        return pipeline

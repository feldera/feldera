import dbsp_api_client
import yaml

from dbsp_api_client.models.pipeline_config import PipelineConfig
from dbsp_api_client.models.pipeline_config_inputs import PipelineConfigInputs
from dbsp_api_client.models.pipeline_config_outputs import PipelineConfigOutputs
from dbsp_api_client.models.new_pipeline_request import NewPipelineRequest
from dbsp_api_client.models.transport_config import TransportConfig
from dbsp_api_client.models.format_config import FormatConfig
from dbsp_api_client.models.input_endpoint_config import InputEndpointConfig
from dbsp_api_client.models.output_endpoint_config import OutputEndpointConfig
from dbsp_api_client.models.kafka_input_config import KafkaInputConfig
from dbsp_api_client.models.kafka_output_config import KafkaOutputConfig
from dbsp_api_client.models.file_input_config import FileInputConfig
from dbsp_api_client.models.file_output_config import FileOutputConfig
from dbsp_api_client.models.csv_parser_config import CsvParserConfig
from dbsp_api_client.models.csv_parser_config import CsvParserConfig
from dbsp_api_client.models.new_config_request import NewConfigRequest
from dbsp_api_client.models.update_config_request import UpdateConfigRequest
from dbsp_api_client.models.csv_encoder_config import CsvEncoderConfig
from dbsp_api_client.api.config import new_config
from dbsp_api_client.api.config import update_config
from dbsp_api_client.api.pipeline import new_pipeline
from dbsp.pipeline import DBSPPipeline
from dbsp.project import DBSPProject

class DBSPPipelineConfig:
    """Pipeline configuration specified by the user when creating
    a new pipeline instance."""

    def __init__(self, project: DBSPProject, workers: int):
        self.project = project
        self.api_client = self.project.api_client
        self.pipeline_config = PipelineConfig(
            workers = workers,
            inputs = PipelineConfigInputs(),
            outputs = PipelineConfigOutputs()
        )
        self.config_id = None
        self.config_version = None
        # self.workers = workers
        # print("config: " + str(self.pipeline_config))

    def add_input(self, name: str, input_endpoint_config: InputEndpointConfig):
        """Add an input endpoint to the pipeline configuration.

        Args:
            name (str): Endpoint name (must be unique across input endpoints).
            input_endpoint_config (InputEndpointConfig): Endpoint configuration.
        """

        # print("yaml:\n" + str(yaml.dump(input_endpoint_config.to_dict())))
        self.pipeline_config.inputs[name] = input_endpoint_config

    def add_output(self, name: str, output_endpoint_config: OutputEndpointConfig):
        """Add an output endpoint to the pipeline configuration.

        Args:
            name (str): Endpoint name (must be unique across output endpoints).
            output_endpoint_config (OutputEndpointConfig): Endpoint configuration.
        """

        self.pipeline_config.outputs[name] = output_endpoint_config

    def yaml(self) -> str:
        """Convert pipeline configuration to YAML format."""

        return yaml.dump(self.pipeline_config.to_dict())

    def run(self) -> DBSPPipeline:
        """Launch a new pipeline.

        Create and run a new pipeline for the specified configuration.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPPipeline
        """

        # print("yaml:\n" + self.yaml())
        if self.config_id == None:
            body = NewConfigRequest(
                project_id = self.project.project_id,
                name = '<anon>',
                config = self.yaml(),
            )
            response = new_config.sync_detailed(client = self.api_client, json_body = body).unwrap("Failed to create pipeline config")
            self.config_id = response.config_id
            self.config_version = response.version
        else:
            body = UpdateConfigRequest(
                config_id = self.config_id,
                name = '<anon>',
                config = self.yaml(),
            )
            response = update_config.sync_detailed(client = self.api_client, json_body = body).unwrap("Failed to update pipeline config")
            self.config_version = response.version

        body = NewPipelineRequest(
            config_id = self.config_id,
            project_id = self.project.project_id,
            config_version = self.config_version,
            project_version = self.project.project_version
        )

        response = new_pipeline.sync_detailed(client = self.api_client, json_body = body).unwrap("Failed to start pipeline")

        return DBSPPipeline(self.api_client, response.pipeline_id)

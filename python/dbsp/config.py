import dbsp_api_client
import yaml
import sys

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
from dbsp_api_client.models.new_config_request import NewConfigRequest
from dbsp_api_client.models.update_config_request import UpdateConfigRequest
from dbsp_api_client.models.csv_encoder_config import CsvEncoderConfig
from dbsp_api_client.api.config import new_config
from dbsp_api_client.api.config import update_config
from dbsp_api_client.api.pipeline import new_pipeline
from dbsp.pipeline import DBSPPipeline
from dbsp.project import DBSPProject
from dbsp.error import TimeoutException

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

    def add_file_input(self, stream: str, filepath: str, format_: FormatConfig, **file_config_options):
        """Add an input endpoint that reads data from a file to the pipeline configuration.

        Args:
            stream (str): Input stream name to connect the endpoint to.
            filepath (str): File to read data from.
            format_ (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
            file_config_options: Additional configuration options defined in FileInputConfig.
        """
        self.add_input(
                stream,
                InputEndpointConfig(
                    stream = stream,
                    transport = TransportConfig(
                        name = "file",
                        config = FileInputConfig.from_dict({ 'path': filepath } | file_config_options)),
                    format_ = format_))

    def add_output(self, name: str, output_endpoint_config: OutputEndpointConfig):
        """Add an output endpoint to the pipeline configuration.

        Args:
            name (str): Endpoint name (must be unique across output endpoints).
            output_endpoint_config (OutputEndpointConfig): Endpoint configuration.
        """

        self.pipeline_config.outputs[name] = output_endpoint_config

    def add_file_output(self, stream: str, filepath: str, format_: FormatConfig, **file_config_options):
        """Add an output endpoint that reads data from a file to the pipeline configuration.

        Args:
            stream (str): Output stream name to connect the endpoint to.
            filepath (str): File to write to.
            format_ (FormatConfig): Data format specification, e.g., CsvOutputFormatConfig().
            file_config_options: Additional configuration options defined in FileOutputConfig.
        """
        self.add_output(
                stream,
                OutputEndpointConfig(
                    stream = stream,
                    transport = TransportConfig(
                        name = "file",
                        config = FileOutputConfig.from_dict({ 'path': filepath } | file_config_options)),
                    format_ = format_))

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

    def run_to_completion(self, timeout: float = sys.maxsize):
        """Launch a new pipeline, wait for it to run to completion, and delete the pipeline.

        This method should only be used for pipelines configured with finite input streams, e.g., files.

        Raises:
            httpx.TimeoutException: If the DBSP server takes longer than Client.timeout to reply to a request.
            dbsp.DBSPServerError: If the DBSP server returns an error.
            dbsp.TimeoutException: If the pipeline does not terminate within 'timeout' seconds.
        """
        pipeline = self.run()
        try:
            pipeline.wait(timeout)
        except TimeoutException as e:
            pipeline.delete()
            raise
            
        pipeline.delete()

class CsvInputFormatConfig(FormatConfig):
    def __init__(self):
         super().__init__('csv', CsvParserConfig())


class CsvOutputFormatConfig(FormatConfig):
    def __init__(self, **csv_config_options):
        super().__init__('csv', CsvEncoderConfig.from_dict(csv_config_options))

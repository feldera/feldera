import uuid
import dbsp_api_client
import yaml
import sys

from dbsp_api_client.models.pipeline_config import PipelineConfig
from dbsp_api_client.models.pipeline_config_inputs import PipelineConfigInputs
from dbsp_api_client.models.pipeline_config_outputs import PipelineConfigOutputs
from dbsp_api_client.models.new_pipeline_request import NewPipelineRequest
from dbsp_api_client.models.transport_config import TransportConfig
from dbsp_api_client.models.format_config import FormatConfig
from dbsp_api_client.models.kafka_input_config import KafkaInputConfig
from dbsp_api_client.models.kafka_output_config import KafkaOutputConfig
from dbsp_api_client.models.file_input_config import FileInputConfig
from dbsp_api_client.models.file_output_config import FileOutputConfig
from dbsp_api_client.models.new_config_request import NewConfigRequest
from dbsp_api_client.models.update_config_request import UpdateConfigRequest
from dbsp_api_client.models.attached_connector import AttachedConnector
from dbsp_api_client.models.direction import Direction
from dbsp_api_client.models.connector_type import ConnectorType
from dbsp_api_client.api.config import new_config
from dbsp_api_client.api.config import update_config
from dbsp_api_client.api.pipeline import new_pipeline
from dbsp.pipeline import DBSPPipeline
from dbsp.project import DBSPProject
from dbsp.error import TimeoutException
from dbsp.connector import DBSPConnector


class DBSPPipelineConfig:
    """Pipeline configuration specified by the user when creating
    a new pipeline instance."""

    def __init__(self, project: DBSPProject, workers: int, name: str = '<anon>', description: str = ''):
        self.project = project
        self.api_client = self.project.api_client
        self.pipeline_config = PipelineConfig(
            workers=workers,
            inputs=PipelineConfigInputs(),
            outputs=PipelineConfigOutputs()
        )
        self.config_id = None
        self.config_version = None
        self.attached_connectors = []
        self.pipeline_id = None
        self.name = name
        self.description = description

    def add_input(self, stream: str, connector: DBSPConnector):
        """Add an input endpoint to the pipeline configuration.

        Args:
            stream (str): Input name (what table to connect to).
            connector (DBSPConnector): Connector configuration.
        """
        connector.save()

        self.attached_connectors.append(AttachedConnector(
            connector_id=connector.connector_id,
            direction=Direction.INPUT,
            uuid=uuid.uuid4().hex,
            config=stream,
        ))

    def add_kafka_input(self, name: str, stream: str, config: KafkaInputConfig, format: FormatConfig):
        """Add an input connector that reads data from Kafka to the pipeline configuration.

        Args:
            name (str): Name of the input connector.
            stream (str): The table name to connect to.
            config (KafkaInputConfig): Config for the new Kafka connector.
            format (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
        """
        self.add_input(
            stream,
            DBSPConnector(self.api_client, name, ConnectorType.KAFKAIN, TransportConfig(
                name="kafka",
                config=config), format=format))

    def add_kafka_output(self, name: str, stream: str, config: KafkaOutputConfig, format: FormatConfig):
        """Add a Kafka output connector to the pipeline configuration.

        Args:
            name (str): Name of the output connector.
            stream (str): The view which produces the output for the connector.
            config (KafkaOutputConfig): Config for the new Kafka connector.
            format (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
        """
        self.add_output(
            stream,
            DBSPConnector(self.api_client, name, ConnectorType.KAFKAOUT, TransportConfig(
                name="kafka",
                config=config), format=format))

    def add_output(self, stream: str, connector: DBSPConnector):
        """Add an output connector to the pipeline configuration.

        Args:
            stream (str): What view to connect to the endpoint.
            connector (DBSPConnector): Connector configuration.
        """
        connector.save()

        self.attached_connectors.append(AttachedConnector(
            connector_id=connector.connector_id,
            direction=Direction.OUTPUT,
            uuid=uuid.uuid4().hex,
            config=stream,
        ))

    def add_file_input(self, stream: str, filepath: str, format: FormatConfig):
        """Add an input connector that reads data from a file to the pipeline configuration.

        Args:
            stream (str): Input table the connector is connected to.
            filepath (str): File to read data from.
            format (FormatConfig): Data format specification, e.g., CsvInputFormatConfig().
        """
        self.add_input(
            stream,
            DBSPConnector(self.api_client, filepath, ConnectorType.FILE, TransportConfig(
                name="file",
                config=FileInputConfig.from_dict(dict({'path': filepath}))), format=format))

    def add_file_output(self, stream: str, filepath: str, format: FormatConfig):
        """Add an output connector that reads data from a file to the pipeline configuration.

        Args:
            stream (str): What view the connector is connected to.
            filepath (str): File to write to.
            format (FormatConfig): Data format specification, e.g., CsvOutputFormatConfig().
        """
        self.add_output(
            stream,
            DBSPConnector(self.api_client, filepath, ConnectorType.FILE, TransportConfig(
                name="file",
                config=FileOutputConfig.from_dict(dict({'path': filepath}))), format=format))

    def yaml(self) -> str:
        """Convert pipeline configuration to YAML format."""
        config = self.pipeline_config.to_dict().copy()
        del config['inputs']
        del config['outputs']
        return yaml.dump(config)

    def save(self):
        "Save the pipeline configuration to DBSP."
        # print("yaml:\n" + self.yaml())
        if self.config_id == None:
            body = NewConfigRequest(
                project_id=self.project.project_id,
                name=self.name,
                description=self.description,
                config=self.yaml(),
                connectors=self.attached_connectors,
            )
            response = new_config.sync_detailed(client=self.api_client, json_body=body).unwrap(
                "Failed to create pipeline config")
            self.config_id = response.config_id
            self.config_version = response.version
        else:
            body = UpdateConfigRequest(
                config_id=self.config_id,
                project_id=self.project.project_id,
                name=self.name,
                description=self.description,
                config=self.yaml(),
                connectors=self.attached_connectors,
            )
            response = update_config.sync_detailed(
                client=self.api_client, json_body=body).unwrap("Failed to update pipeline config")
            self.config_version = response.version

    def run(self) -> DBSPPipeline:
        """Launch a new pipeline.

        Create and run a new pipeline for the specified configuration.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPPipeline
        """
        if self.pipeline_id != None:
            raise Exception("Pipeline is already running for this config")
        self.save()

        body = NewPipelineRequest(
            config_id=self.config_id,
            config_version=self.config_version,
        )
        response = new_pipeline.sync_detailed(
            client=self.api_client, json_body=body).unwrap("Failed to start pipeline")
        self.pipeline_id = response.pipeline_id

        return DBSPPipeline(self, self.api_client, response.pipeline_id)

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

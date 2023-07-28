from typing import Any, Dict
import dbsp_api_client
import yaml

from dbsp_api_client.models.pipeline_config import PipelineConfig
from dbsp_api_client.models.pipeline_config_inputs import PipelineConfigInputs
from dbsp_api_client.models.pipeline_config_outputs import PipelineConfigOutputs
from dbsp_api_client.models.transport_config import TransportConfig
from dbsp_api_client.models.format_config import FormatConfig
from dbsp_api_client.models.input_endpoint_config import InputEndpointConfig
from dbsp_api_client.models.output_endpoint_config import OutputEndpointConfig
from dbsp_api_client.models.kafka_input_config import KafkaInputConfig
from dbsp_api_client.models.kafka_output_config import KafkaOutputConfig
from dbsp_api_client.models.file_input_config import FileInputConfig
from dbsp_api_client.models.file_output_config import FileOutputConfig
from dbsp_api_client.models.csv_parser_config import CsvParserConfig
from dbsp_api_client.models.new_connector_request import NewConnectorRequest
from dbsp_api_client.models.update_connector_request import UpdateConnectorRequest
from dbsp_api_client.models.csv_encoder_config import CsvEncoderConfig
from dbsp_api_client.models.connector_config import ConnectorConfig
from dbsp_api_client.api.connectors import new_connector
from dbsp_api_client.api.connectors import update_connector
from dbsp_api_client.api.connectors import delete_connector
from dbsp_api_client.api.connectors import list_connectors
from dbsp_api_client.models.connector_descr import ConnectorDescr


class DBSPConnector:
    "A connector that can be attached to configs."

    def __init__(self, api_client, name: str, transport: "TransportConfig", format: "FormatConfig", description: str = ''):
        self.api_client = api_client
        # If the connector already exists we make sure to get its id so it will
        # just update on save
        response = list_connectors.sync_detailed(
            client=self.api_client, name=name)
        if isinstance(response.parsed, list):
            self.connector_id = response.parsed[0].connector_id
        else:
            self.connector_id = None

        self.name = name
        self.description = description
        self.transport = transport
        self.format = format
        self.config = self.to_dict()

    def to_dict(self) -> Dict[str, Any]:
        transport_ = self.transport.to_dict()
        format_ = self.format.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(
            {
                "format": format_,
                "transport": transport_,
            }
        )

        return field_dict

    def save(self):
        "Save the connector or update it if it already exists."
        if self.connector_id is None:
            body = NewConnectorRequest(
                name=self.name,
                description=self.description,
                config=ConnectorConfig.from_dict(self.config),
            )
            response = new_connector.sync_detailed(
                client=self.api_client, json_body=body).unwrap("Failed to create the connector")
            self.connector_id = response.connector_id
        else:
            body = UpdateConnectorRequest(
                name=self.name,
                description=self.description,
                config=ConnectorConfig.from_dict(self.config),
            )
            response = update_connector.sync_detailed(
                connector_id=self.connector_id,
                client=self.api_client, json_body=body).unwrap("Failed to update the connector")

    def delete(self):
        "Delete the existing connector."
        if self.connector_id is not None:
            delete_connector.sync_detailed(connector_id=self.connector_id).unwrap(
                "Failed to add the connector")


class CsvInputFormatConfig(FormatConfig):
    def __init__(self):
        super().__init__('csv', CsvParserConfig())


class CsvOutputFormatConfig(FormatConfig):
    def __init__(self, **csv_config_options):
        super().__init__('csv', CsvEncoderConfig.from_dict(csv_config_options))

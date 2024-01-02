from typing import Any, Dict
import feldera_api_client
import yaml

from feldera_api_client.models.pipeline_config import PipelineConfig
from feldera_api_client.models.pipeline_config_inputs import PipelineConfigInputs
from feldera_api_client.models.pipeline_config_outputs import PipelineConfigOutputs
from feldera_api_client.models.transport_config import TransportConfig
from feldera_api_client.models.format_config import FormatConfig
from feldera_api_client.models.input_endpoint_config import InputEndpointConfig
from feldera_api_client.models.output_endpoint_config import OutputEndpointConfig
from feldera_api_client.models.url_input_config import UrlInputConfig
from feldera_api_client.models.kafka_input_config import KafkaInputConfig
from feldera_api_client.models.kafka_output_config import KafkaOutputConfig
from feldera_api_client.models.file_input_config import FileInputConfig
from feldera_api_client.models.file_output_config import FileOutputConfig
from feldera_api_client.models.csv_parser_config import CsvParserConfig
from feldera_api_client.models.json_parser_config import JsonParserConfig
from feldera_api_client.models.new_connector_request import NewConnectorRequest
from feldera_api_client.models.update_connector_request import UpdateConnectorRequest
from feldera_api_client.models.csv_encoder_config import CsvEncoderConfig
from feldera_api_client.models.json_encoder_config import JsonEncoderConfig
from feldera_api_client.models.connector_config import ConnectorConfig
from feldera_api_client.api.connectors import new_connector
from feldera_api_client.api.connectors import update_connector
from feldera_api_client.api.connectors import delete_connector
from feldera_api_client.api.connectors import list_connectors
from feldera_api_client.models.connector_descr import ConnectorDescr


class DBSPConnector:
    "A connector that can be attached to configs."

    def __init__(
        self,
        api_client,
        name: str,
        transport: "TransportConfig",
        format: "FormatConfig",
        description: str = "",
    ):
        self.api_client = api_client
        # If the connector already exists we make sure to get its id so it will
        # just update on save
        response = list_connectors.sync_detailed(client=self.api_client, name=name)
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
                client=self.api_client, json_body=body
            ).unwrap("Failed to create the connector")
            self.connector_id = response.connector_id
        else:
            body = UpdateConnectorRequest(
                name=self.name,
                description=self.description,
                config=ConnectorConfig.from_dict(self.config),
            )
            response = update_connector.sync_detailed(
                connector_name=self.name, client=self.api_client, json_body=body
            ).unwrap("Failed to update the connector")

    def delete(self):
        "Delete the existing connector."
        if self.connector_id is not None:
            delete_connector.sync_detailed(connector_name=self.connector_name).unwrap(
                "Failed to add the connector"
            )


class CsvInputFormatConfig(FormatConfig):
    def __init__(self):
        super().__init__("csv", CsvParserConfig())


class CsvOutputFormatConfig(FormatConfig):
    def __init__(self, **csv_config_options):
        super().__init__("csv", CsvEncoderConfig.from_dict(csv_config_options))


class JsonInputFormatConfig(FormatConfig):
    def __init__(self, **json_config_options):
        super().__init__("json", JsonParserConfig.from_dict(json_config_options))


class JsonOutputFormatConfig(FormatConfig):
    def __init__(self, **json_config_options):
        super().__init__("json", JsonEncoderConfig.from_dict(json_config_options))

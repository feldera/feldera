from typing import Optional, Mapping
from typing_extensions import Self
from enum import Enum
import json
from abc import ABC

class Format(ABC):
    """
    Base class for all data formats.
    """

    def to_dict(self) -> dict:
        """
        Serialize to a dict to be used in the API request.

        :meta private:
        """
        raise NotImplementedError


class JSONUpdateFormat(Enum):
    """
    Supported JSON data change event formats.

    Each element in a JSON-formatted input stream specifies
    an update to one or more records in an input table.  We support
    several different ways to represent such updates.

    https://www.feldera.com/docs/api/json/#the-insertdelete-format
    """

    InsertDelete = 1
    """
    Insert/delete format.
    
    Each element in the input stream consists of an "insert" or "delete"
    command and a record to be inserted to or deleted from the input table.
    
    Example: `{"insert": {"id": 1, "name": "Alice"}, "delete": {"id": 2, "name": "Bob"}}`
    Here, `id` and `name` are the columns in the table.
    """

    Raw = 2
    """
    Raw input format.
    
    This format is suitable for insert-only streams (no deletions).
    Each element in the input stream contains a record without any
    additional envelope that gets inserted in the input table.
    
    Example: `{"id": 1, "name": "Alice"}`
    Here, `id` and `name` are the columns in the table.
    """

    def __str__(self):
        match self:
            case JSONUpdateFormat.InsertDelete:
                return "insert_delete"
            case JSONUpdateFormat.Raw:
                return "raw"


class JSONFormat(Format):
    """
    Used to represent data ingested and output from Feldera in the JSON format.
    """

    def __init__(self, config: Optional[dict] = None):
        """
        Creates a new JSONFormat instance.

        :param config: Optional. Configuration for the JSON format.
        """

        self.config: dict = config or {
            "array": False,
        }

    def with_update_format(self, update_format: JSONUpdateFormat) -> Self:
        """
        Specifies the format of the data change events in the JSON data stream.
        """

        self.config["update_format"] = update_format.__str__()
        return self

    def with_array(self, array: bool) -> Self:
        """
        Set to `True` if updates in this stream are packaged into JSON arrays.

        Example: `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
        """

        self.config["array"] = array
        return self

    def to_dict(self):
        """
        Serialize to a dict to be used in the API request.

        :meta private:
        """
        return {
            "name": "json",
            "config": self.config
        }


class CSVFormat(Format):
    """
    Used to represent data ingested and output from Feldera in the CSV format.
    
    https://www.feldera.com/docs/api/csv
    """

    def to_dict(self) -> dict:
        """
        Serialize to a dict to be used in the API request.

        :meta private:
        """
        return {
            "name": "csv"
        }


class AvroFormat(Format):
    """
    Avro output format configuration.

    :param config:
        A dictionary that contains the entire configuration for the Avro format.
    :param schema:
        Avro schema used to encode output records.
        Specified as a string containing schema definition in JSON format.
        This schema must match precisely the SQL view definition, including nullability of columns.
    :param skip_schema_id:
        Set `True` if the serialized message should only contain the data and
        not contain the magic byte + schema ID. `False` by default.
        The first 5 bytes of the Avro message are the magic byte and 4-byte schema ID.
        https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
    :param registry_urls:
        List of schema registry URLs. When non-empty, the connector will
        post the schema to the registry and use the schema id returned
        by the registry.  Otherwise, schema id 0 is used.
    :param registry_headers:
        Custom headers that will be added to every call to the schema registry.
        This option requires `registry_urls` to be set.
    :param registry_proxy:
        Proxy that will be used to access the schema registry.
        Requires `registry_urls` to be set.
    :param registry_timeout_secs:
        Timeout in seconds used to connect to the registry.
        Requires `registry_urls` to be set.
    :param registry_username:
        Username used to authenticate with the registry.
        Requires `registry_urls` to be set. This option is mutually exclusive with
        token-based authentication (see `registry_authorization_token`).
    :param registry_password:
        Password used to authenticate with the registry.
        Requires `registry_urls` to be set.
    :param registry_authorization_token:
        Token used to authenticate with the registry.
        Requires `registry_urls` to be set.  This option is mutually exclusive with
        password-based authentication (see `registry_username` and `registry_password`).
    """

    def __init__(
            self,
            config: Optional[dict] = None,
            schema: Optional[str] = None,
            skip_schema_id: Optional[bool] = None,
            registry_urls: Optional[list[str]] = None,
            registry_headers: Optional[Mapping[str, str]] = None,
            registry_proxy: Optional[str] = None,
            registry_timeout_secs: Optional[int] = None,
            registry_username: Optional[str] = None,
            registry_password: Optional[str] = None,
            registry_authorization_token: Optional[str] = None,
    ):
        config = config or {}
        self.__dict__.update(config)

        self.schema = schema
        self.skip_schema_id = skip_schema_id
        self.registry_urls = registry_urls
        self.registry_headers = registry_headers
        self.registry_proxy = registry_proxy
        self.registry_timeout_secs = registry_timeout_secs
        self.registry_username = registry_username
        self.registry_password = registry_password
        self.registry_authorization_token = registry_authorization_token

    def with_schema(self, schema: str | dict) -> Self:
        """
        Avro schema used to encode output records.

        Specified as a string containing schema definition in JSON format.
        This schema must match precisely the SQL view definition, including nullability of columns.
        """
        if isinstance(schema, dict):
            schema = json.dumps(schema)

        self.schema = schema
        return self

    def with_skip_schema_id(self, skip_schema_id: bool) -> Self:
        """
        Set `True` if the serialized message should only contain the data and
        not contain the magic byte + schema ID. `False` by default.

        The first 5 bytes of the Avro message are the magic byte and 4-byte schema ID.
        https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
        """
        self.skip_schema_id = skip_schema_id
        return self

    def with_registry_urls(self, registry_urls: list[str]) -> Self:
        """
        List of schema registry URLs.

        When non-empty, the connector will post the schema to the registry and use the schema id returned
        by the registry.  Otherwise, schema id 0 is used.
        """
        self.registry_urls = registry_urls
        return self

    def with_registry_headers(self, registry_headers: Mapping[str, str]) -> Self:
        """
        Custom headers that will be added to every call to the schema registry.

        This option requires `registry_urls` to be set.
        """
        self.registry_headers = registry_headers
        return self

    def with_registry_proxy(self, registry_proxy: str) -> Self:
        """
        Proxy that will be used to access the schema registry.

        Requires `registry_urls` to be set.
        """
        self.registry_proxy = registry_proxy
        return self

    def with_registry_timeout_secs(self, registry_timeout_secs: int) -> Self:
        """
        Timeout in seconds used to connect to the registry.

        Requires `registry_urls` to be set.
        """
        if registry_timeout_secs < 0:
            raise ValueError("registry_timeout_secs must be a positive integer")

        self.registry_timeout_secs = registry_timeout_secs
        return self

    def with_registry_username(self, registry_username: str) -> Self:
        """
        Username used to authenticate with the registry.

        Requires `registry_urls` to be set. This option is mutually exclusive with
        token-based authentication (see `registry_authorization_token`).
        """
        if self.registry_authorization_token is not None:
            raise ValueError("registry_username is mutually exclusive with registry_authorization_token")

        self.registry_username = registry_username
        return self

    def with_registry_password(self, registry_password: str) -> Self:
        """
        Password used to authenticate with the registry.

        Requires `registry_urls` to be set. This option is mutually exclusive with
        token-based authentication (see `registry_authorization_token`).
        """
        if self.registry_authorization_token is not None:
            raise ValueError("registry_password is mutually exclusive with registry_authorization_token")

        self.registry_password = registry_password
        return self

    def with_registry_authorization_token(self, registry_authorization_token: str) -> Self:
        """
        Token used to authenticate with the registry.

        Requires `registry_urls` to be set. This option is mutually exclusive with
        password-based authentication (see `registry_username` and `registry_password`).
        """
        if self.registry_username is not None or self.registry_password is not None:
            raise ValueError("registry_authorization_token is mutually exclusive with registry_username")

        self.registry_authorization_token = registry_authorization_token
        return self

    def to_dict(self) -> dict:
        """
        Serialize to a dict to be used in the API request.

        :meta private:
        """
        return {
            "name": "avro",
            "config": self.__dict__
        }

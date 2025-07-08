import pathlib
from typing import Any, Dict, Optional
import logging
import time
import json
from decimal import Decimal
from typing import Generator

from feldera.rest.config import Config
from feldera.rest.feldera_config import FelderaConfig
from feldera.rest.errors import FelderaTimeoutError
from feldera.rest.pipeline import Pipeline
from feldera.rest._httprequests import HttpRequests
from feldera.rest._helpers import client_version


def _validate_no_none_keys_in_map(data):
    def validate_no_none_keys(d: Dict[Any, Any]) -> None:
        for k, v in d.items():
            if isinstance(v, dict) and any(k is None for k in v.keys()):
                raise ValueError("keys of SQL MAP objects cannot be NULL")

    if isinstance(data, list):
        for datum in data:
            validate_no_none_keys(datum)
    elif isinstance(data, dict):
        validate_no_none_keys(data)
    else:
        return


def _prepare_boolean_input(value: bool) -> str:
    return "true" if value else "false"


class FelderaClient:
    """
    A client for the Feldera HTTP API

    A client instance is needed for every Feldera API method to know the
    location of Feldera and its permissions.
    """

    def __init__(
        self,
        url: str,
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
        requests_verify: bool = True,
    ) -> None:
        """
        :param url: The url to Feldera API (ex: https://try.feldera.com)
        :param api_key: The optional API key for Feldera
        :param timeout: (optional) The amount of time in seconds that the client
            will wait for a response before timing
            out.
        :param requests_verify: The `verify` parameter passed to the requests
            library. `True` by default.
        """

        self.config = Config(
            url, api_key, timeout=timeout, requests_verify=requests_verify
        )
        self.http = HttpRequests(self.config)

        try:
            config = self.get_config()
            version = client_version()
            if config.version != version:
                logging.warn(
                    f"Client is on version {version} while server is at "
                    f"{config.version}. There could be incompatibilities."
                )
        except Exception as e:
            logging.error(f"Failed to connect to Feldera API: {e}")
            raise e

    @staticmethod
    def localhost(port: int = 8080) -> "FelderaClient":
        """
        Create a FelderaClient that connects to the local Feldera instance
        """

        return FelderaClient(f"http://127.0.0.1:{port}")

    def get_pipeline(self, pipeline_name) -> Pipeline:
        """
        Get a pipeline by name

        :param pipeline_name: The name of the pipeline
        """

        resp = self.http.get(f"/pipelines/{pipeline_name}")

        return Pipeline.from_dict(resp)

    def get_runtime_config(self, pipeline_name) -> dict:
        """
        Get the runtime config of a pipeline by name

        :param pipeline_name: The name of the pipeline
        """

        resp: dict = self.http.get(f"/pipelines/{pipeline_name}")

        return resp.get("runtime_config")

    def pipelines(self) -> list[Pipeline]:
        """
        Get all pipelines
        """

        resp = self.http.get(
            path="/pipelines",
        )

        return [Pipeline.from_dict(pipeline) for pipeline in resp]

    def __wait_for_compilation(self, name: str):
        wait = ["Pending", "CompilingSql", "SqlCompiled", "CompilingRust"]

        while True:
            p = self.get_pipeline(name)
            status = p.program_status

            if status == "Success":
                return p
            elif status not in wait:
                # error handling for SQL compilation errors
                if status == "SqlError":
                    sql_errors = p.program_error["sql_compilation"]["messages"]
                    if sql_errors:
                        err_msg = f"Pipeline {name} failed to compile:\n"
                        for sql_error in sql_errors:
                            err_msg += (
                                f"{sql_error['error_type']}\n{sql_error['message']}\n"
                            )
                            err_msg += f"Code snippet:\n{sql_error['snippet']}"
                        raise RuntimeError(err_msg)

                raise RuntimeError(f"The program failed to compile: {status}")

            logging.debug("still compiling %s, waiting for 100 more milliseconds", name)
            time.sleep(0.1)

    def create_pipeline(self, pipeline: Pipeline) -> Pipeline:
        """
        Create a pipeline if it doesn't exist and wait for it to compile


        :name: The name of the pipeline
        """

        body = {
            "name": pipeline.name,
            "program_code": pipeline.program_code,
            "udf_rust": pipeline.udf_rust,
            "udf_toml": pipeline.udf_toml,
            "program_config": pipeline.program_config,
            "runtime_config": pipeline.runtime_config,
            "description": pipeline.description or "",
        }

        self.http.post(
            path="/pipelines",
            body=body,
        )

        return self.__wait_for_compilation(pipeline.name)

    def create_or_update_pipeline(self, pipeline: Pipeline) -> Pipeline:
        """
        Create a pipeline if it doesn't exist or update a pipeline and wait for
        it to compile
        """

        body = {
            "name": pipeline.name,
            "program_code": pipeline.program_code,
            "udf_rust": pipeline.udf_rust,
            "udf_toml": pipeline.udf_toml,
            "program_config": pipeline.program_config,
            "runtime_config": pipeline.runtime_config,
            "description": pipeline.description or "",
        }

        self.http.put(
            path=f"/pipelines/{pipeline.name}",
            body=body,
        )

        return self.__wait_for_compilation(pipeline.name)

    def patch_pipeline(self, name: str, sql: str):
        """
        Incrementally update the pipeline SQL

        :param name: The name of the pipeline
        :param sql: The SQL snippet. Replaces the existing SQL code with this
            one.
        """

        self.http.patch(
            path=f"/pipelines/{name}",
            body={"program_code": sql},
        )

    def delete_pipeline(self, name: str):
        """
        Deletes a pipeline by name

        :param name: The name of the pipeline
        """
        self.http.delete(
            path=f"/pipelines/{name}",
        )

    def get_pipeline_stats(self, name: str) -> dict:
        """
        Get the pipeline metrics and performance counters

        :param name: The name of the pipeline
        """

        resp = self.http.get(
            path=f"/pipelines/{name}/stats",
        )

        return resp

    def start_pipeline(self, pipeline_name: str, timeout_s: Optional[float] = 300):
        """

        :param pipeline_name: The name of the pipeline to start
        :param timeout_s: The amount of time in seconds to wait for the pipeline
            to start. 300 seconds by default.
        """

        if timeout_s is None:
            timeout_s = 300

        self.http.post(
            path=f"/pipelines/{pipeline_name}/start",
        )

        start_time = time.monotonic()

        while True:
            if timeout_s is not None:
                elapsed = time.monotonic() - start_time
                if elapsed > timeout_s:
                    raise TimeoutError(
                        f"Timed out waiting for pipeline {pipeline_name} to start"
                    )

            resp = self.get_pipeline(pipeline_name)
            status = resp.deployment_status

            if status == "Running":
                break
            elif status == "Failed":
                raise RuntimeError(
                    f"""Unable to START the pipeline.
Reason: The pipeline is in a STOPPED state due to the following error:
{resp.deployment_error.get("message", "")}"""
                )

            logging.debug(
                "still starting %s, waiting for 100 more milliseconds", pipeline_name
            )
            time.sleep(0.1)

    def pause_pipeline(
        self,
        pipeline_name: str,
        error_message: str = None,
        timeout_s: Optional[float] = 300,
    ):
        """
        Stop a pipeline

        :param pipeline_name: The name of the pipeline to stop
        :param error_message: The error message to show if the pipeline is in
             STOPPED state due to a failure.
        :param timeout_s: The amount of time in seconds to wait for the pipeline
            to pause. 300 seconds by default.
        """

        if timeout_s is None:
            timeout_s = 300

        self.http.post(
            path=f"/pipelines/{pipeline_name}/pause",
        )

        if error_message is None:
            error_message = "Unable to PAUSE the pipeline.\n"

        start_time = time.monotonic()

        while True:
            if timeout_s is not None:
                elapsed = time.monotonic() - start_time
                if elapsed > timeout_s:
                    raise TimeoutError(
                        f"Timed out waiting for pipeline {pipeline_name} to pause"
                    )

            resp = self.get_pipeline(pipeline_name)
            status = resp.deployment_status

            if status == "Paused":
                break
            elif (
                status == "Stopped"
                and len(resp.deployment_error or {}) > 0
                and resp.deployment_desired_status == "Stopped"
            ):
                raise RuntimeError(
                    error_message
                    + f"""Reason: The pipeline is in a STOPPED state due to the following error:
{resp.deployment_error.get("message", "")}"""
                )

            logging.debug(
                "still pausing %s, waiting for 100 more milliseconds", pipeline_name
            )
            time.sleep(0.1)

    def stop_pipeline(
        self, pipeline_name: str, force: bool, timeout_s: Optional[float] = 300
    ):
        """
        Stop a pipeline

        :param pipeline_name: The name of the pipeline to stop
        :param force: Set True to immediately scale compute resources to zero.
            Set False to automatically checkpoint before stopping.
        :param timeout_s: The amount of time in seconds to wait for the pipeline
            to stop. Default is 300 seconds.
        """

        if timeout_s is None:
            timeout_s = 300

        params = {"force": str(force).lower()}

        self.http.post(
            path=f"/pipelines/{pipeline_name}/stop",
            params=params,
        )

        start = time.monotonic()

        while time.monotonic() - start < timeout_s:
            status = self.get_pipeline(pipeline_name).deployment_status

            if status == "Stopped":
                return

            logging.debug(
                "still stopping %s, waiting for 100 more milliseconds",
                pipeline_name,
            )
            time.sleep(0.1)

        raise FelderaTimeoutError(
            f"timeout error: pipeline '{pipeline_name}' did not stop in {timeout_s} seconds"
        )

    def clear_storage(self, pipeline_name: str, timeout_s: Optional[float] = 300):
        """
        Clears the storage from the pipeline.
        This operation cannot be canceled.

        :param pipeline_name: The name of the pipeline
        :param timeout_s: The amount of time in seconds to wait for the storage
            to clear. Default is 300 seconds.
        """
        if timeout_s is None:
            timeout_s = 300

        self.http.post(
            path=f"/pipelines/{pipeline_name}/clear",
        )

        start = time.monotonic()

        while time.monotonic() - start < timeout_s:
            status = self.get_pipeline(pipeline_name).storage_status

            if status == "Cleared":
                return

            logging.debug(
                "still clearing %s, waiting for 100 more milliseconds",
                pipeline_name,
            )
            time.sleep(0.1)

        raise FelderaTimeoutError(
            f"timeout error: pipeline '{pipeline_name}' did not clear storage in {timeout_s} seconds"
        )

    def checkpoint_pipeline(self, pipeline_name: str) -> int:
        """
        Checkpoint a fault-tolerant pipeline

        :param pipeline_name: The name of the pipeline to checkpoint
        """

        resp = self.http.post(
            path=f"/pipelines/{pipeline_name}/checkpoint",
        )

        return int(resp.get("checkpoint_sequence_number"))

    def checkpoint_pipeline_status(self, pipeline_name: str) -> dict:
        """
        Gets the checkpoint status

        :param pipeline_name: The name of the pipeline to check the checkpoint status of.
        """

        return self.http.get(path=f"/pipelines/{pipeline_name}/checkpoint_status")

    def sync_checkpoint(self, pipeline_name: str) -> str:
        """
        Triggers a checkpoint synchronization for the specified pipeline.
        Check the status by calling `pipeline_sync_checkpoint_status`.

        :param pipeline_name: Name of the pipeline whose checkpoint should be synchronized.
        """

        resp = self.http.post(
            path=f"/pipelines/{pipeline_name}/checkpoint/sync",
        )

        return resp.get("checkpoint_uuid")

    def sync_checkpoint_status(self, pipeline_name: str) -> dict:
        """
        Gets the checkpoint sync status of the pipeline

        :param pipeline_name: The name of the pipeline to check the checkpoint synchronization status of.
        """

        return self.http.get(
            path=f"/pipelines/{pipeline_name}/checkpoint/sync_status",
        )

    def push_to_pipeline(
        self,
        pipeline_name: str,
        table_name: str,
        format: str,
        data: list[list | str | dict] | dict | str,
        array: bool = False,
        force: bool = False,
        update_format: str = "raw",
        json_flavor: Optional[str] = None,
        serialize: bool = True,
    ):
        """
        Insert data into a pipeline

        :param pipeline_name: The name of the pipeline
        :param table_name: The name of the table
        :param format: The format of the data, either "json" or "csv"
        :param array: True if updates in this stream are packed into JSON arrays, used in conjunction with the
            "json" format

        :param force: If True, the data will be inserted even if the pipeline is paused
        :param update_format: JSON data change event format, used in conjunction with the "json" format,
            the default value is "insert_delete", other supported formats: "weighted", "debezium", "snowflake", "raw"
        :param json_flavor: JSON encoding used for individual table records, the default value is "default", other supported encodings:
            "debezium_mysql", "snowflake", "kafka_connect_json_converter", "pandas"
        :param data: The data to insert
        :param serialize: If True, the data will be serialized to JSON. True by default
        """

        if format not in ["json", "csv"]:
            raise ValueError("format must be either 'json' or 'csv'")

        if update_format not in [
            "insert_delete",
            "weighted",
            "debezium",
            "snowflake",
            "raw",
        ]:
            raise ValueError(
                "update_format must be one of 'insert_delete', 'weighted', 'debezium', 'snowflake', 'raw'"
            )

        if json_flavor is not None and json_flavor not in [
            "default",
            "debezium_mysql",
            "snowflake",
            "kafka_connect_json_converter",
            "pandas",
        ]:
            raise ValueError(
                "json_flavor must be one of 'default', 'debezium_mysql', 'snowflake', 'kafka_connect_json_converter', 'pandas'"
            )

        if update_format == "insert_delete":
            if array:
                for datum in data:
                    _validate_no_none_keys_in_map(datum.get("insert", {}))
                    _validate_no_none_keys_in_map(datum.get("delete", {}))
            else:
                data: dict = data
                _validate_no_none_keys_in_map(data.get("insert", {}))
                _validate_no_none_keys_in_map(data.get("delete", {}))
        else:
            _validate_no_none_keys_in_map(data)

        # python sends `True` which isn't accepted by the backend
        array = _prepare_boolean_input(array)
        force = _prepare_boolean_input(force)

        params = {
            "force": force,
            "format": format,
        }

        if format == "json":
            params["array"] = array
            params["update_format"] = update_format

        if json_flavor is not None:
            params["json_flavor"] = json_flavor

        content_type = "application/json"

        if format == "csv":
            content_type = "text/csv"
            data = bytes(str(data), "utf-8")

        self.http.post(
            path=f"/pipelines/{pipeline_name}/ingress/{table_name}",
            params=params,
            content_type=content_type,
            body=data,
            serialize=serialize,
        )

    def listen_to_pipeline(
        self,
        pipeline_name: str,
        table_name: str,
        format: str,
        backpressure: bool = True,
        array: bool = False,
        timeout: Optional[float] = None,
    ):
        """
        Listen for updates to views for pipeline, yields the chunks of data

        :param pipeline_name: The name of the pipeline
        :param table_name: The name of the table to listen to
        :param format: The format of the data, either "json" or "csv"
        :param backpressure: When the flag is True (the default), this method waits for the consumer to receive each
            chunk and blocks the pipeline if the consumer cannot keep up. When this flag is False, the pipeline drops
            data chunks if the consumer is not keeping up with its output. This prevents a slow consumer from slowing
            down the entire pipeline.
        :param array: Set True to group updates in this stream into JSON arrays, used in conjunction with the
            "json" format, the default value is False

        :param timeout: The amount of time in seconds to listen to the stream for
        """

        params = {
            "format": format,
            "backpressure": _prepare_boolean_input(backpressure),
        }

        if format == "json":
            params["array"] = _prepare_boolean_input(array)

        resp = self.http.post(
            path=f"/pipelines/{pipeline_name}/egress/{table_name}",
            params=params,
            stream=True,
        )

        end = time.monotonic() + timeout if timeout else None

        # Using the default chunk size below makes `iter_lines` extremely
        # inefficient when dealing with long lines.
        for chunk in resp.iter_lines(chunk_size=50000000):
            if end and time.monotonic() > end:
                break
            if chunk:
                yield json.loads(chunk, parse_float=Decimal)

    def query_as_text(
        self, pipeline_name: str, query: str
    ) -> Generator[str, None, None]:
        """
        Executes an ad-hoc query on the specified pipeline and returns a generator that yields lines of the table.

        :param pipeline_name: The name of the pipeline to query.
        :param query: The SQL query to be executed.
        :return: A generator yielding the query result in tabular format, one line at a time.
        """
        params = {
            "pipeline_name": pipeline_name,
            "sql": query,
            "format": "text",
        }

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/query",
            params=params,
            stream=True,
        )

        chunk: bytes
        for chunk in resp.iter_lines(chunk_size=50000000):
            if chunk:
                yield chunk.decode("utf-8")

    def query_as_parquet(self, pipeline_name: str, query: str, path: str):
        """
        Executes an ad-hoc query on the specified pipeline and saves the result to a parquet file.
        If the extension isn't `parquet`, it will be automatically appended to `path`.

        :param pipeline_name: The name of the pipeline to query.
        :param query: The SQL query to be executed.
        :param path: The path including the file name to save the resulting parquet file in.
        """

        params = {
            "pipeline_name": pipeline_name,
            "sql": query,
            "format": "parquet",
        }

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/query",
            params=params,
            stream=True,
        )

        path: pathlib.Path = pathlib.Path(path)

        ext = ".parquet"
        if path.suffix != ext:
            path = path.with_suffix(ext)

        file = open(path, "wb")

        chunk: bytes
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)
        file.close()

    def query_as_json(
        self, pipeline_name: str, query: str
    ) -> Generator[dict, None, None]:
        """
        Executes an ad-hoc query on the specified pipeline and returns the result as a generator that yields
        rows of the query as Python dictionaries.
        All floating-point numbers are deserialized as Decimal objects to avoid precision loss.

        :param pipeline_name: The name of the pipeline to query.
        :param query: The SQL query to be executed.
        :return: A generator that yields each row of the result as a Python dictionary, deserialized from JSON.
        """
        params = {
            "pipeline_name": pipeline_name,
            "sql": query,
            "format": "json",
        }

        resp = self.http.get(
            path=f"/pipelines/{pipeline_name}/query",
            params=params,
            stream=True,
        )

        for chunk in resp.iter_lines(chunk_size=50000000):
            if chunk:
                yield json.loads(chunk, parse_float=Decimal)

    def pause_connector(self, pipeline_name, table_name, connector_name):
        """
        Pause the specified input connector.

        Connectors allow feldera to fetch data from a source or write data to a sink.
        This method allows users to **PAUSE** a specific **INPUT** connector.
        All connectors are RUNNING by default.

        Refer to the connector documentation for more information:
        <https://docs.feldera.com/connectors/#input-connector-orchestration>

        :param pipeline_name: The name of the pipeline.
        :param table_name: The name of the table associated with this connector.
        :param connector_name: The name of the connector.

        :raises FelderaAPIError: If the connector cannot be found, or if the pipeline is not running.
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/pause",
        )

    def resume_connector(
        self, pipeline_name: str, table_name: str, connector_name: str
    ):
        """
        Resume the specified connector.

        Connectors allow feldera to fetch data from a source or write data to a sink.
        This method allows users to **RESUME / START** a specific **INPUT** connector.
        All connectors are RUNNING by default.

        Refer to the connector documentation for more information:
        <https://docs.feldera.com/connectors/#input-connector-orchestration>

        :param pipeline_name: The name of the pipeline.
        :param table_name: The name of the table associated with this connector.
        :param connector_name: The name of the connector.

        :raises FelderaAPIError: If the connector cannot be found, or if the pipeline is not running.
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/tables/{table_name}/connectors/{connector_name}/start",
        )

    def get_config(self) -> FelderaConfig:
        """
        Get general feldera configuration.
        """

        resp = self.http.get(path="/config")

        return FelderaConfig(resp)

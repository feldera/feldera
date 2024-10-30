import pathlib
from typing import Optional
import logging
import time
import json
from decimal import Decimal
from typing import Generator

from feldera.rest.config import Config
from feldera.rest.pipeline import Pipeline
from feldera.rest._httprequests import HttpRequests


def _prepare_boolean_input(value: bool) -> str:
    return "true" if value else "false"


class FelderaClient:
    """
    A client for the Feldera HTTP API

    A client instance is needed for every Feldera API method to know the location of
    Feldera and its permissions.
    """

    def __init__(
        self,
        url: str,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> None:
        """
        :param url: The url to Feldera API (ex: https://try.feldera.com)
        :param api_key: The optional API key for Feldera
        :param timeout: (optional) The amount of time in seconds that the client will wait for a response before timing
            out.
        """

        self.config = Config(url, api_key, timeout)
        self.http = HttpRequests(self.config)

        try:
            self.pipelines()
        except Exception as e:
            logging.error(f"Failed to connect to Feldera API: {e}")
            raise e

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
        wait = ["Pending", "CompilingSql", "CompilingRust"]

        while True:
            p = self.get_pipeline(name)
            status = p.program_status

            if status == "Success":
                return p
            elif status not in wait:
                # error handling for SQL compilation errors
                if isinstance(status, dict):
                    sql_errors = status.get("SqlError")
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
        Create a pipeline if it doesn't exist or update a pipeline and wait for it to compile
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
        :param sql: The SQL snippet. Replaces the existing SQL code with this one.
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

    def start_pipeline(self, pipeline_name: str):
        """
        Start a pipeline

        :param pipeline_name: The name of the pipeline to start
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/start",
        )

        while True:
            status = self.get_pipeline(pipeline_name).deployment_status

            if status == "Running":
                break
            elif status == "Failed":
                raise RuntimeError("Failed to start pipeline")

            logging.debug(
                "still starting %s, waiting for 100 more milliseconds", pipeline_name
            )
            time.sleep(0.1)

    def pause_pipeline(self, pipeline_name: str):
        """
        Stop a pipeline

        :param pipeline_name: The name of the pipeline to stop
        """
        self.http.post(
            path=f"/pipelines/{pipeline_name}/pause",
        )

        while True:
            status = self.get_pipeline(pipeline_name).deployment_status

            if status == "Paused":
                break
            elif status == "Failed":
                raise RuntimeError("Failed to pause pipeline")

            logging.debug(
                "still pausing %s, waiting for 100 more milliseconds", pipeline_name
            )
            time.sleep(0.1)

    def shutdown_pipeline(self, pipeline_name: str):
        """
        Shutdown a pipeline

        :param pipeline_name: The name of the pipeline to shut down
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/shutdown",
        )

        start = time.time()
        timeout = 15

        while time.time() - start < timeout:
            status = self.get_pipeline(pipeline_name).deployment_status

            if status == "Shutdown":
                return

            logging.debug(
                "still shutting down %s, waiting for 100 more milliseconds",
                pipeline_name,
            )
            time.sleep(0.1)

        # retry sending shutdown request as the pipline hasn't shutdown yet
        logging.debug(
            "pipeline %s hasn't shutdown after %s s, retrying", pipeline_name, timeout
        )
        self.http.post(
            path=f"/pipelines/{pipeline_name}/shutdown",
        )

        start = time.time()
        timeout = 5

        while time.time() - start < timeout:
            status = self.get_pipeline(pipeline_name).deployment_status

            if status == "Shutdown":
                return

            logging.debug(
                "still shutting down %s, waiting for 100 more milliseconds",
                pipeline_name,
            )
            time.sleep(0.1)

        raise RuntimeError(f"Failed to shutdown pipeline {pipeline_name}")

    def checkpoint_pipeline(self, pipeline_name: str):
        """
        Checkpoint a fault-tolerant pipeline

        :param pipeline_name: The name of the pipeline to checkpoint
        """

        self.http.post(
            path=f"/pipelines/{pipeline_name}/checkpoint",
        )

    def push_to_pipeline(
        self,
        pipeline_name: str,
        table_name: str,
        format: str,
        data: list[list | str | dict],
        array: bool = False,
        force: bool = False,
        update_format: str = "raw",
        json_flavor: str = None,
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

        end = time.time() + timeout if timeout else None

        # Using the default chunk size below makes `iter_lines` extremely
        # inefficient when dealing with long lines.
        for chunk in resp.iter_lines(chunk_size=50000000):
            if end and time.time() > end:
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

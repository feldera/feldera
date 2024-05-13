from typing import Optional
import logging
import time

from feldera.config import Config
from feldera.connector import Connector
from feldera.attached_connector import AttachedConnector
from feldera.program import Program
from feldera.pipeline import Pipeline
from feldera._httprequests import HttpRequests


class Client:
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
        :param timeout: (optional) The amount of time in seconds that the cient will wait for a response beforing timing
            out.
        """

        self.config = Config(url, api_key, timeout)
        self.http = HttpRequests(self.config)

    def programs(self) -> list[Program]:
        """
        Get all programs
        """
        resp = self.http.get(
            path="/programs",
        )

        return [
            Program(
                name=program.get("name"),
                program=program.get("code"),
                description=program.get("description"),
            ) for program in resp
        ]

    def get_program(self, name: str, with_code: bool = False) -> Program:
        """
        Get a program by name
        :param name: The name of the program
        :param with_code: If True, the program code will be included in the response
        """
        resp = self.http.get(
            path=f"/programs/{name}",
            params={
                "with_code": with_code,
            }
        )

        return Program(
            name=resp.get("name"),
            program=resp.get("code"),
            description=resp.get("description"),
            status=resp.get("status"),
            version=resp.get("version"),
        )

    def compile_program(self, program: Program):
        """
        Compiles a SQL program
        :param program: The program to compile
        """
        body = {
            "code": program.program,
            "description": program.description or "",
        }

        resp = self.http.put(
            path=f"/programs/{program.name}",
            body=body,
        )
        program.id = resp.get("program_id")
        program.version = resp.get("version")

        while True:
            status = self.get_program(program.name).status

            if status == "Success":
                break
            elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
                raise RuntimeError(f"Failed program compilation with status {status}")

            logging.debug("still compiling %s, waiting for 5 more seconds", program.name)
            time.sleep(5)

    def delete_program(self, name: str):
        """
        Deletes a program by name
        :param name: The name of the program
        """

        self.http.delete(
            path=f"/programs/{name}",
        )

    def connectors(self) -> list[Connector]:
        """
        Get all connectors
        """

        resp = self.http.get(
            path="/connectors",
        )

        return [
            Connector(
                name=connector.get("name"),
                description=connector.get("description"),
                config=connector.get("config"),
                id=connector.get("connector_id"),
            ) for connector in resp
        ]

    def get_connector(self, name: str) -> Connector:
        """
        Get a connector by name
        :param name: The name of the connector
        """

        resp = self.http.get(
            path=f"/connectors/{name}",
        )

        return Connector(
            name=resp.get("name"),
            description=resp.get("description"),
            config=resp.get("config"),
            id=resp.get("connector_id"),
        )

    def create_connector(self, connector: Connector):
        """
        Create a connector
        :param connector: The connector to create
        """
        body = {
            "description": connector.description or "",
            "config": connector.config
        }

        resp = self.http.put(
            path=f"/connectors/{connector.name}",
            body=body,
        )

        connector.id = resp.get("connector_id")

    def delete_connector(self, name: str):
        """
        Deletes a connector by name
        :param name: The name of the connector
        """
        self.http.delete(
            path=f"/connectors/{name}",
        )

    @staticmethod
    def __pipeline_from_dict(pipeline: dict) -> Pipeline:
        attached_connectors = pipeline.get("attached_connectors")
        descriptor = pipeline.get("descriptor")
        return Pipeline(
            name=descriptor.get("name"),
            program_name=descriptor.get("program_name"),
            version=descriptor.get("version"),
            description=descriptor.get("description"),
            id=descriptor.get("id"),
            config=descriptor.get("config"),
            state=pipeline.get("state"),
            attached_connectors=[
                AttachedConnector(
                    connector_name=con.get("connector_name"),
                    is_input=con.get("is_input"),
                    relation_name=con.get("relation_name"),
                    name=con.get("name"),
                )
                for con in attached_connectors
            ] if attached_connectors else []
        )

    def pipelines(self) -> list[Pipeline]:
        """
        Get all pipelines
        """

        resp = self.http.get(
            path="/pipelines",
        )

        return [self.__pipeline_from_dict(pipeline) for pipeline in resp]

    def get_pipeline(self, name: str) -> Pipeline:
        """
        Get a pipeline by name
        """

        resp = self.http.get(
            path=f"/pipelines/{name}",
        )

        return self.__pipeline_from_dict(resp)

    def create_pipeline(self, pipeline: Pipeline):
        """
        Create a pipeline
        """

        body = {
            "config": pipeline.config,
            "description": pipeline.description or "",
            "connectors": pipeline.attached_connectors,
            "program_name": pipeline.program_name,
        }

        resp = self.http.put(
            path=f"/pipelines/{pipeline.name}",
            body=body,
        )

        pipeline.id = resp.get("pipeline_id")

    def get_pipeline_config(self, name: str) -> dict:
        """
        Get the configuration of a pipeline by name
        """

        resp = self.http.get(
            path=f"/pipelines/{name}/config",
        )

        return resp

    def validate_pipeline(self, name: str) -> bool:
        """
        Validate a pipeline.
        Checks whether the pipeline is configured correctly.
        This includes checking whether the pipeline references a valid compiled program,
        whether the connectors reference valid tables/views in the program, and more.
        """

        resp = self.http.get(
            path=f"/pipelines/{name}/validate",
        )

        return "success" in resp

    def delete_pipeline(self, name: str):
        """
        Deletes a pipeline by name
        :param name: The name of the pipeline
        """
        resp = self.http.delete(
            path=f"/pipelines/{name}",
        )

    def get_pipeline_stats(self, name: str) -> dict:
        """
        Get the pipeline metrics and performance counters
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
            status = self.get_pipeline(pipeline_name).state.get("current_status")

            if status == "Running":
                break
            elif status == "Failed":
                raise RuntimeError(f"Failed to start pipeline")

            logging.debug("still starting %s, waiting for 100 more milliseconds", pipeline_name)
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
            status = self.get_pipeline(pipeline_name).state.get("current_status")

            if status == "Paused":
                break
            elif status == "Failed":
                raise RuntimeError(f"Failed to pause pipeline")

            logging.debug("still pausing %s, waiting for 100 more milliseconds", pipeline_name)
            time.sleep(0.1)

    def shutdown_pipeline(self, pipeline_name: str):
        """
        Shutdown a pipeline
        :param pipeline_name: The name of the pipeline to shutdown
        """
        self.http.post(
            path=f"/pipelines/{pipeline_name}/shutdown",
        )

        while True:
            status = self.get_pipeline(pipeline_name).state.get("current_status")

            if status == "Shutdown":
                break
            elif status == "Failed":
                raise RuntimeError(f"Failed to shutdown pipeline")

            logging.debug("still shutting down %s, waiting for 100 more milliseconds", pipeline_name)
            time.sleep(0.1)

    # TODO: better name for this method
    def push_to_pipeline(
            self,
            pipeline_name: str,
            table_name: str,
            format: str,
            data: list[list | str | dict],
            array: bool = False,
            force: bool = False,
            update_format: str = "raw",
    ):
        """
        Insert data into a pipeline
        :param pipeline_name: The name of the pipeline
        :param table_name: The name of the table
        :param format: The format of the data, either "json" or "csv"
        :param array: Set True if updates in this stream are packed into JSON arrays, used in conjunction with the
            "json" format
        :param force: If True, the data will be inserted even if the pipeline is paused
        :param update_format: JSON data change event format, used in conjunction with the "json" format,
        the default value is "insert_delete", other supported formats: "weighted", "debezium", "snowflake", "raw"
        :param data: The data to insert
        """

        if format not in ["json", "csv"]:
            raise ValueError("format must be either 'json' or 'csv'")

        if update_format not in ["insert_delete", "weighted", "debezium", "snowflake", "raw"]:
            raise ValueError("update_format must be one of 'insert_delete', 'weighted', 'debezium', 'snowflake', 'raw'")

        # python sends `True` which isn't accepted by the backend
        array = "true" if array else "false"
        force = "true" if force else "false"

        params = {
            "force": force,
            "format": format,
        }

        if format == "json":
            params["array"] = array
            params["update_format"] = update_format

        content_type = "application/json"

        self.start_pipeline(pipeline_name)

        if format == "csv":
            content_type = "text/csv"
            data = bytes(str(data), "utf-8")

        self.http.post(
            path=f"/pipelines/{pipeline_name}/ingress/{table_name}",
            params=params,
            content_type=content_type,
            body=data,
        )

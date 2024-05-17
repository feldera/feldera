import time
import pandas
import re

from typing import Optional, Dict
from typing_extensions import Self
from queue import Queue

from feldera import FelderaClient
from feldera.rest.program import Program
from feldera.rest.pipeline import Pipeline
from feldera.rest.connector import Connector
from feldera._sql_table import SQLTable
from feldera.sql_schema import SQLSchema
from feldera.output_handler import OutputHandler
from feldera.output_handler import _OutputHandlerInstruction
from enum import Enum


class BuildMode(Enum):
    CREATE = 1
    GET = 2
    GET_OR_CREATE = 3


def _table_name_from_sql(ddl: str) -> str:
    return re.findall(r"[\w']+", ddl)[2]


class SQLContext:
    """
    The SQLContext is the main entry point for the Feldera SQL API.
    Abstracts the interaction with the Feldera API and provides a high-level interface for SQL pipelines.
    """
    client: FelderaClient
    pipeline_name: str
    program_name: str
    build_mode: BuildMode

    pipeline_description: str = ""
    program_description: str = ""
    ddl: str = ""
    views: Dict[str, str] = {}
    tables: Dict[str, SQLTable] = {}
    todo_tables: Dict[str, Optional[SQLTable]] = {}
    http_input_buffer: list[Dict[str, dict | list[dict] | str]] = []

    # buffer that stores all input connectors to be created
    # this is a Mapping[table_name -> list[Connector]]
    input_connectors_buffer: Dict[str, list[Connector]] = {}

    # buffer that stores all output connectors to be created
    # this is a Mapping[view_name -> list[Connector]]
    output_connectors_buffer: Dict[str, list[Connector]] = {}

    views_tx: list[Dict[str, Queue]] = []

    def __init__(
            self,
            pipeline_name: str,
            client: FelderaClient,
            pipeline_description: str = None,
            program_name: str = None,
            program_description: str = None,
    ):
        self.client = client

        self.pipeline_name = pipeline_name
        self.pipeline_description = pipeline_description or ""

        self.program_name = program_name or pipeline_name
        self.program_description = program_description or ""

    def __build_ddl(self):
        """
        Internal function used to create the DDL from the registered tables and views.
        """
        tables = "\n".join([tbl.build_ddl() for tbl in self.tables.values()])
        views = "\n".join([view for view in self.views.values()])

        self.ddl = tables + "\n" + views

    def __setup(self):
        """
        Internal function used to setup the pipeline and program on the Feldera API.
        """

        self.__build_ddl()

        # TODO: handle different build modes

        program = Program(self.program_name, self.ddl, self.program_description)

        self.client.compile_program(program)

        attached_cons = []

        for tbl_name, conns in self.input_connectors_buffer.items():
            for conn in conns:
                self.client.create_connector(conn)
                attached_con = conn.attach_relation(tbl_name, True)
                attached_cons.append(attached_con)

        for view_name, conns in self.output_connectors_buffer.items():
            for con in conns:
                self.client.create_connector(con)
                attached_con = con.attach_relation(view_name, False)
                attached_cons.append(attached_con)

        pipeline = Pipeline(
            self.pipeline_name,
            self.program_name,
            self.pipeline_description,
            attached_connectors=attached_cons
        )

        self.client.create_pipeline(pipeline)

    def create(self) -> Self:
        """
        Sets the build mode to CREATE, meaning that the pipeline will be created from scratch.
        """

        self.build_mode = BuildMode.CREATE
        return self

    def get(self) -> Self:
        """
        Sets the build mode to GET, meaning that an existing pipeline will be used.
        """

        self.build_mode = BuildMode.GET
        return self

    def get_or_create(self) -> Self:
        """
        Sets the build mode to GET_OR_CREATE, meaning that an existing pipeline will be used if it exists,
        else a new one will be created.
        """

        self.build_mode = BuildMode.GET_OR_CREATE
        return self

    def register_table(self, table_name: str, schema: Optional[SQLSchema] = None, ddl: str = None):
        """
        Registers a table with the SQLContext. The table can be registered with a schema or with the SQL DDL.
        One of the two must be provided, but not both.
        Auto inserts the trailing semicolon if not present.
        In the future, schema will be inferred from the data provided from applicable sources.

        :param table_name: The name of the table.
        :param schema: The schema of the table.
        :param ddl: The SQL DDL of the table.
        """

        if not schema and not ddl:
            raise ValueError("Schema inference isn't supported yet, either provide a schema or the SQL DDL")

        if schema and ddl:
            raise ValueError("Provide either a schema or the SQL DDL, not both")

        if ddl:
            self.register_table_from_sql(ddl)
            return

        if schema:
            self.tables[table_name] = SQLTable(table_name, schema=schema)
        else:
            self.todo_tables[table_name] = None

    def register_table_from_sql(self, ddl: str):
        """
        Registers a table with the provided SQL DDL.
        Auto inserts the trailing semicolon if not present.

        :param ddl: The SQL DDL of the table.
        """

        if ddl[-1] != ';':
            ddl += ';'

        name = _table_name_from_sql(ddl)

        self.tables[name] = SQLTable(name, ddl)

    def input_from_pandas(self, table_name: str, df: pandas.DataFrame):
        """
        Adds a pandas DataFrame to the input buffer of the SQLContext, to be pushed to the pipeline.

        :param table_name: The name of the table.
        :param df: The pandas DataFrame to be pushed to the pipeline.
        """

        tbl = self.tables.get(table_name)

        if tbl:
            # tbl.validate_schema(df)   TODO: something like this would be nice
            self.http_input_buffer.append({tbl.name: df.to_dict('records')})
            return

        tbl = self.todo_tables.get(table_name)

        if not tbl:
            raise ValueError(f"Table {table_name} not registered")

        # tbl.infer_schema(df)  TODO: support schema inference

        self.tables[table_name] = tbl
        self.todo_tables.pop(table_name)

        self.http_input_buffer.append({tbl.name: df.to_dict('records')})

    def register_view(self, name: str, query: str):
        """
        Registers a Feldera View based on the provided query.
        Auto inserts the trailing semicolon if not present.

        :param name: The name of the view.
        :param query: The query to be used to create the view.
        """

        if query[-1] != ';':
            query += ';'

        self.views[name] = f"CREATE VIEW {name} AS {query}"

    def listen(self, view_name: str) -> OutputHandler:
        """
        Listens to the output of the provided view so that it is available in the notebook / python code.

        :param view_name: The name of the view to listen to.
        """

        queue = Queue(maxsize=1)

        self.views_tx.append({view_name: queue})

        handler = OutputHandler(self.client, self.pipeline_name, view_name, queue)
        handler.start()

        return handler

    def from_delta_table(self, table_name: str, connector_name: str, config: dict):
        """
        Tells feldera to read the data from the specified delta table.

        :param table_name: The name of the table.
        :param connector_name: The unique name for this connector.
        :param config: The configuration for the delta table.
        """

        # TODO: test this, can't test this right now, because of a serialization issue
        # TODO: see: https://github.com/feldera/feldera/pull/1764

        if config.get("uri") is None:
            raise ValueError("uri is required in the config")

        if config.get("mode") is None:
            raise ValueError("mode is required in the config, valid modes: snapshot, follow, snapshot_and_follow")

        if config.get("mode") not in ["snapshot", "follow", "snapshot_and_follow"]:
            raise ValueError("mode must be one of snapshot, follow, snapshot_and_follow")

        connector = Connector(name=connector_name,
                              config={
                                  "transport": {
                                      "name": "delta_table_input",
                                      "config": config,
                                  }
                              })

        if table_name in self.input_connectors_buffer:
            self.output_connectors_buffer[table_name].append(connector)
        else:
            self.output_connectors_buffer[table_name] = [connector]

    def to_delta_table(self, view_name: str, connector_name: str, config: dict):
        """
        Tells feldera to write the data to the specified delta table.

        :param view_name: The name of the view whose output is sent to delta table.
        :param connector_name: The unique name for this connector.
        :param config: The configuration for the delta table connector.
        """

        if config.get("uri") is None:
            raise ValueError("uri is required in the config")

        connector = Connector(name=connector_name,
                              config={
                                  "transport": {
                                      "name": "delta_table_output",
                                      "config": config,
                                  }
                              })

        if view_name in self.output_connectors_buffer:
            self.output_connectors_buffer[view_name].append(connector)
        else:
            self.output_connectors_buffer[view_name] = [connector]

    def run_to_completion(self):
        """
        Runs the pipeline to completion, waiting for all input records to be processed.

        :raises RuntimeError: If the pipeline returns unknown metrics.
        """

        self.__setup()

        self.client.start_pipeline(self.pipeline_name)

        for view_queue in self.views_tx:
            for view_name, queue in view_queue.items():
                queue.put(_OutputHandlerInstruction.PipelineStarted)
                queue.join()

        for input_buffer in self.http_input_buffer:
            for tbl_name, data in input_buffer.items():
                self.client.push_to_pipeline(self.pipeline_name, tbl_name, "json", data, array=True)

        self.http_input_buffer.clear()

        while True:
            metrics: dict = self.client.get_pipeline_stats(self.pipeline_name).get("global_metrics")
            pipeline_complete: bool = metrics.get("pipeline_complete")

            if not pipeline_complete:
                raise RuntimeError("received unknown metrics from the pipeline")

            if pipeline_complete:
                break

            time.sleep(1)

        for view_queue in self.views_tx:
            for view_name, queue in view_queue.items():
                queue.put(_OutputHandlerInstruction.RanToCompletion)
                queue.join()

    def start(self):
        # TODO: implement this later
        pass

    def pause(self):
        """
        Pauses the pipeline.
        """

        self.client.pause_pipeline(self.pipeline_name)

    def shutdown(self):
        """
        Pauses and shuts down the pipeline.
        """

        self.client.pause_pipeline(self.pipeline_name)
        self.client.shutdown_pipeline(self.pipeline_name)

    def resume(self):
        """
        Resumes the pipeline.
        """

        self.client.start_pipeline(self.pipeline_name)

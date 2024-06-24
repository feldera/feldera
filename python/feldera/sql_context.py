import time
import pandas
import re

from typing import Optional, Dict, Callable

import pandas as pd
from typing_extensions import Self
from queue import Queue

from feldera.rest.errors import FelderaAPIError
from feldera import FelderaClient
from feldera.rest.program import Program
from feldera.rest.pipeline import Pipeline
from feldera.rest.connector import Connector
from feldera._sql_table import SQLTable
from feldera._sql_view import SQLView
from feldera.sql_schema import SQLSchema
from feldera.output_handler import OutputHandler
from feldera._callback_runner import CallbackRunner, _CallbackRunnerInstruction
from feldera._helpers import ensure_dataframe_has_columns
from feldera.formats import JSONFormat, CSVFormat, AvroFormat
from feldera.resources import Resources
from feldera.enums import BuildMode, CompilationProfile, PipelineStatus
from feldera._helpers import validate_connector_input_format, chunk_dataframe


def _table_name_from_sql(ddl: str) -> str:
    return re.findall(r"[\w']+", ddl)[2]


class SQLContext:
    """
    .. _SQLContext:

    The SQLContext is the main entry point for the Feldera SQL API.
    Abstracts the interaction with the Feldera API and provides a high-level interface for SQL pipelines.

    :param pipeline_name: The name of the pipeline.
    :param client: The :class:`.FelderaClient` instance to use.
    :param pipeline_description: The description of the pipeline.
    :param program_name: The name of the program. Defaults to the pipeline name.
    :param program_description: The description of the program. Defaults to an empty string.
    :param storage: Set `True` to use storage with this pipeline. Defaults to False.
    :param workers: The number of workers to use with this pipeline. Defaults to 8.
    :param resources: The :class:`.PipelineResourceConfig` for the pipeline. Defaults to None.
    :param compilation_profile: The compilation profile to use when compiling the program. Defaults to
        :class:`.CompilationProfile.OPTIMIZED`.
    """

    def __init__(
            self,
            pipeline_name: str,
            client: FelderaClient,
            pipeline_description: str = None,
            program_name: str = None,
            program_description: str = None,
            storage: bool = False,
            workers: int = 8,
            resources: Resources = None,
            compilation_profile: CompilationProfile = CompilationProfile.OPTIMIZED
    ):
        self.build_mode: Optional[BuildMode] = None

        self.ddl: str = ""

        # In the SQL DDL declaration, the order of the tables and views is important.
        # From python 3.7 onwards, the order of insertion is preserved in dictionaries.
        # https://softwaremaniacs.org/blog/2020/02/05/dicts-ordered/en/
        self.views: Dict[str, SQLView] = {}
        self.tables: Dict[str, SQLTable] = {}
        self.types: Dict[str, str] = {}

        # buffer that stores all input connectors to be created
        # this is a Mapping[table_name -> list[Connector]]
        self.input_connectors_buffer: Dict[str, list[Connector]] = {}

        # buffer that stores all output connectors to be created
        # this is a Mapping[view_name -> list[Connector]]
        self.output_connectors_buffer: Dict[str, list[Connector]] = {}

        self.views_tx: list[Dict[str, Queue]] = []

        self.client: FelderaClient = client

        self.pipeline_name: str = pipeline_name
        self.pipeline_description: str = pipeline_description or ""

        self.program_name: str = program_name or pipeline_name
        self.program_description: str = program_description or ""
        self.storage: bool = storage
        self.workers: int = workers
        self.resources: Resources = resources
        self.compilation_profile: CompilationProfile = compilation_profile

    def __build_ddl(self):
        """
        Internal function used to create the DDL from the registered tables and views.
        """
        types = "\n\n".join([type for type in self.types.values()])
        tables = "\n\n".join([tbl.build_ddl() for tbl in self.tables.values()])
        views = "\n\n".join([view.build_ddl() for view in self.views.values()])

        self.ddl = types + "\n\n" + tables + "\n\n" + views

    def __setup_pipeline(self):
        """
        Internal function used to setup the pipeline and program on the Feldera API.

        :meta private:
        """

        self.__build_ddl()

        # TODO: handle different build modes

        program = Program(self.program_name, self.ddl, self.program_description)

        self.client.compile_program(program, {
            "profile": self.compilation_profile.value
        })

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

        config = { 'storage': self.storage, 'workers': self.workers }
        if self.resources:
            config["resources"] = self.resources.__dict__

        pipeline = Pipeline(
            self.pipeline_name,
            self.program_name,
            self.pipeline_description,
            config=config,
            attached_connectors=attached_cons
        )

        self.client.create_pipeline(pipeline)

    def __setup_output_listeners(self):
        """
        Internal function used to setup the output listeners.

        :meta private:
        """

        for view_queue in self.views_tx:
            for view_name, queue in view_queue.items():
                # sends a message to the callback runner to start listening
                queue.put(_CallbackRunnerInstruction.PipelineStarted)
                # block until the callback runner is ready
                queue.join()

    def create(self) -> Self:
        """
        Set the build mode to CREATE, meaning that the pipeline will be created from scratch.
        """

        self.build_mode = BuildMode.CREATE
        return self

    def get(self) -> Self:
        """
        Set the build mode to GET, meaning that an existing pipeline will be used.
        """

        self.build_mode = BuildMode.GET
        return self

    def get_or_create(self) -> Self:
        """
        Set the build mode to GET_OR_CREATE, meaning that an existing pipeline will be used if it exists,
        else a new one will be created.
        """

        self.build_mode = BuildMode.GET_OR_CREATE
        return self

    def pipeline_status(self) -> PipelineStatus:
        """
        Return the current state of the pipeline.
        """

        try:
            pipeline = self.client.get_pipeline(self.pipeline_name)
            return PipelineStatus.from_str(pipeline.current_state())

        except FelderaAPIError as err:
            if err.status_code == 404:
                return PipelineStatus.NOT_FOUND
            else:
                raise err

    def register_table(self, table_name: str, schema: Optional[SQLSchema] = None, ddl: str = None):
        """
        Register a table with the SQLContext. The table can be registered with a schema or with the SQL DDL.
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

    def register_table_from_sql(self, ddl: str):
        """
        Register a table with the provided SQL DDL.
        Auto inserts the trailing semicolon if not present.

        :param ddl: The SQL DDL of the table.
        """

        ddl = ddl.strip()
        if ddl[-1] != ';':
            ddl += ';'

        name = _table_name_from_sql(ddl)

        self.tables[name] = SQLTable(name, ddl)

    def input_pandas(self, table_name: str, df: pandas.DataFrame, force: bool = False):
        """
        Push all rows in a pandas DataFrame to the pipeline.

        :param table_name: The name of the table to insert data into.
        :param df: The pandas DataFrame to be pushed to the pipeline.
        :param force: `True` to push data even if the pipeline is paused. `False` by default.
        """

        status = self.pipeline_status()
        if status not in [
            PipelineStatus.RUNNING,
            PipelineStatus.PAUSED,
        ]:
            raise RuntimeError("Pipeline must be running or paused to push data")

        if not force and status == PipelineStatus.PAUSED:
            raise RuntimeError("Pipeline is paused, set force=True to push data")

        ensure_dataframe_has_columns(df)

        tbl = self.tables.get(table_name)

        if tbl is None:
            raise ValueError(f"Cannot push to table '{table_name}' as it is not registered yet")
        else:
            # tbl.validate_schema(df)   TODO: something like this would be nice
            for datum in chunk_dataframe(df):
                self.client.push_to_pipeline(
                    self.pipeline_name,
                    table_name,
                    "json",
                    datum.to_json(orient='records', date_format='epoch'),
                    json_flavor='pandas',
                    array=True,
                    serialize=False,
                    force=force,
                )
            return

    def register_local_view(self, name: str, query: str):
        """
        Register a local view with the SQLContext.
        Local views are not exposed to the outside world as an output of the computation.
        This is useful for modularizing the SQL code, by declaring intermediate views
        that are used in the implementation of other views.

        Marking a view as local results in it not being materialized, potentially yielding performance benefit
        over regular views at the cost of not being able to observe it (e.g., attach connectors to it).
        This is particularly handy for intermediate views that are used in the implementation of other views,
        a practice that benefits modularization of the SQL code.

        Auto inserts the trailing semicolon if not present.

        :param name: The name of the view.
        :param query: The query to be used to create the view.
        """

        self.views[name] = SQLView(name, True, query)

    def register_output_view(self, name: str, query: str):
        """
        Register a Feldera View based on the provided query.
        Auto inserts the trailing semicolon if not present.

        :param name: The name of the view.
        :param query: The query to be used to create the view.
        """

        self.views[name] = SQLView(name, False, query)

    def register_type(self, name: str, spec: str):
        """
        Register a SQL type.
        Auto inserts the trailing semicolon if not present.

        :param name: The name of the type.
        :param spec: Type definition.
        """

        spec = spec.strip()
        if spec[-1] != ';':
            spec += ';'

        self.types[name] = f"CREATE TYPE {name} AS {spec}"

    def add_lateness(self, view: str, timestamp_column: str, lateness_expr: str):
        """
        Add a lateness annotation to a view.
        Lateness annotations are SQL statements of the form
        'LATENESS <view>.<timstamp_column> <lateness_expr>;', e.g.,
        'LATENESS V.COL1 INTERVAL '1' HOUR;'

        :param view: View name.
        :param timestamp_column: Timestamp column to associate lateness with.
        :param lateness_expr: SQL expression that defines lateness.
        """

        view = self.views.get(view)

        view.add_lateness(timestamp_column, lateness_expr)

    def listen(self, view_name: str) -> OutputHandler:
        """
        Listen to the output of the provided view so that it is available in the notebook / python code.

        :param view_name: The name of the view to listen to.
        """

        queue: Optional[Queue] = None

        if self.pipeline_status() != PipelineStatus.RUNNING:
            queue = Queue(maxsize=1)
            self.views_tx.append({view_name: queue})

        handler = OutputHandler(self.client, self.pipeline_name, view_name, queue)
        handler.start()

        return handler

    def connect_source_delta_table(self, table_name: str, connector_name: str, config: dict):
        """
        Tell Feldera to read the data from the specified delta table.

        :param table_name: The name of the table.
        :param connector_name: The unique name for this connector.
        :param config: The configuration for the delta table.
        """

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
            self.input_connectors_buffer[table_name].append(connector)
        else:
            self.input_connectors_buffer[table_name] = [connector]

    def connect_sink_delta_table(self, view_name: str, connector_name: str, config: dict):
        """
        Tell Feldera to write the data to the specified delta table.

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
                                  },
                                  "enable_output_buffer": True,
                                  "max_output_buffer_time_millis": 10000,
                              })

        if view_name in self.output_connectors_buffer:
            self.output_connectors_buffer[view_name].append(connector)
        else:
            self.output_connectors_buffer[view_name] = [connector]

    def foreach_chunk(self, view_name: str, callback: Callable[[pandas.DataFrame, int], None]):
        """
        Run the given callback on each chunk of the output of the specified view.

        :param view_name: The name of the view.
        :param callback: The callback to run on each chunk. The callback should take two arguments:

                - **chunk**  -> The chunk as a pandas DataFrame
                - **seq_no** -> The sequence number. The sequence number is a monotonically increasing integer that
                  starts from 0. Note that the sequence number is unique for each chunk, but not necessarily contiguous.

        Please note that the callback is run in a separate thread, so it should be thread-safe.
        Please note that the callback should not block for a long time, as by default, backpressure is enabled and
        will block the pipeline.

        .. note::
            - The callback must be thread-safe as it will be run in a separate thread.

        """

        queue: Optional[Queue] = None

        if self.pipeline_status() != PipelineStatus.RUNNING:
            queue = Queue(maxsize=1)
            self.views_tx.append({view_name: queue})

        handler = CallbackRunner(self.client, self.pipeline_name, view_name, callback, queue)
        handler.start()

    def connect_source_kafka(
        self,
        table_name: str,
        connector_name: str,
        config: dict,
        fmt: JSONFormat | CSVFormat
    ):
        """
        Associate the specified kafka topics on the specified Kafka server as input source for the specified table in
        Feldera. The table is populated with changes from the specified kafka topics.

        :param table_name: The name of the table.
        :param connector_name: The unique name for this connector.
        :param config: The configuration for the kafka connector.
        :param fmt: The format of the data in the kafka topic.
        """

        if config.get("bootstrap.servers") is None:
            raise ValueError("'bootstrap.servers' is required in the config")

        if config.get("topics") is None:
            raise ValueError("topics is required in the config")

        validate_connector_input_format(fmt)

        connector = Connector(
            name=connector_name,
            config={
                "transport": {
                    "name": "kafka_input",
                    "config": config,
                },
                "format": fmt.to_dict(),
            }
        )

        if table_name in self.input_connectors_buffer:
            self.input_connectors_buffer[table_name].append(connector)
        else:
            self.input_connectors_buffer[table_name] = [connector]

    def connect_sink_kafka(
        self,
        view_name: str,
        connector_name: str,
        config: dict,
        fmt: JSONFormat | CSVFormat | AvroFormat
    ):
        """
        Associate the specified Kafka topic on the specified Kafka server as output sink for the specified view in
        Feldera. The topic is populated with changes in the specified view.

        :param view_name: The name of the view whose changes are sent to kafka topic.
        :param connector_name: The unique name for this connector.
        :param config: The configuration for the kafka connector.
        :param fmt: The format of the data in the kafka topic.
        """

        if config.get("bootstrap.servers") is None:
            raise ValueError("'bootstrap.servers' is required in the config")

        if config.get("topic") is None:
            raise ValueError("topic is required in the config")

        validate_connector_input_format(fmt)

        connector = Connector(
            name=connector_name,
            config={
                "transport": {
                    "name": "kafka_output",
                    "config": config,
                },
                "format": fmt.to_dict(),
            }
        )

        if view_name in self.output_connectors_buffer:
            self.output_connectors_buffer[view_name].append(connector)
        else:
            self.output_connectors_buffer[view_name] = [connector]

    def connect_source_url(
        self,
        table_name: str,
        connector_name: str,
        path: str,
        fmt: JSONFormat | CSVFormat
    ):
        """
        Associate the specified URL as input source for the specified table in Feldera.
        Feldera will make a GET request to the specified URL to read the data and populate the table.

        :param table_name: The name of the table.
        :param connector_name: The unique name for this connector.
        :param path: The URL to read the data from.
        :param fmt: The format of the data in the URL.
        """

        validate_connector_input_format(fmt)

        connector = Connector(
            name=connector_name,
            config={
                "transport": {
                    "name": "url_input",
                    "config": {
                        "path": path
                    }
                },
                "format": fmt.to_dict(),
            }
        )

        if table_name in self.input_connectors_buffer:
            self.input_connectors_buffer[table_name].append(connector)
        else:
            self.input_connectors_buffer[table_name] = [connector]

    def wait_for_completion(self, shutdown: bool = False):
        """
        Block until the pipeline has completed processing all input records.

        This method blocks until (1) all input connectors attached to the pipeline
        have finished reading their input data sources and issued end-of-input
        notifications to the pipeline, and (2) all inputs received from these
        connectors have been fully processed and corresponding outputs have been
        sent out through the output connectors.

        This method will block indefinitely if at least one of the input
        connectors attached to the pipeline is a streaming connector, such as
        Kafka, that does not issue the end-of-input notification.

        :param shutdown: If True, the pipeline will be shutdown after completion. False by default.

        :raises RuntimeError: If the pipeline returns unknown metrics.
        """

        if self.pipeline_status() not in [
            PipelineStatus.RUNNING,
            PipelineStatus.INITIALIZING,
            PipelineStatus.PROVISIONING,
        ]:
            raise RuntimeError("Pipeline must be running to wait for completion")

        while True:
            metrics: dict = self.client.get_pipeline_stats(self.pipeline_name).get("global_metrics")
            pipeline_complete: bool = metrics.get("pipeline_complete")

            if pipeline_complete is None:
                raise RuntimeError("received unknown metrics from the pipeline, pipeline_complete is None")

            if pipeline_complete:
                break

            time.sleep(1)

        if shutdown:
            self.shutdown()

    def start(self):
        """
        .. _start:

        Start the pipeline.

        :raises RuntimeError: If the pipeline returns unknown metrics.
        """

        current_state = self.pipeline_status()
        if current_state not in [PipelineStatus.NOT_FOUND, PipelineStatus.SHUTDOWN]:
            raise RuntimeError(f"pipeline in state: {str(current_state.name)} cannot be started")

        self.__setup_pipeline()

        self.pause()

        self.__setup_output_listeners()

        self.resume()

    def wait_for_idle(
            self,
            idle_interval_s: float = 5.0,
            timeout_s: float = 600.0,
            poll_interval_s: float = 0.2
    ):
        """
        Wait for the pipeline to become idle and then returns.

        Idle is defined as a sufficiently long interval in which the number of
        input and processed records reported by the pipeline do not change, and
        they equal each other (thus, all input records present at the pipeline
        have been processed).

        :param idle_interval_s: Idle interval duration (default is 5.0 seconds).
        :param timeout_s: Timeout waiting for idle (default is 600.0 seconds).
        :param poll_interval_s: Polling interval, should be set substantially
            smaller than the idle interval (default is 0.2 seconds).
        :raises ValueError: If idle interval is larger than timeout, poll interval
            is larger than timeout, or poll interval is larger than idle interval.
        :raises RuntimeError: If the metrics are missing or the timeout was
            reached.
        """
        if idle_interval_s > timeout_s:
            raise ValueError(f"idle interval ({idle_interval_s}s) cannot be larger than timeout ({timeout_s}s)")
        if poll_interval_s > timeout_s:
            raise ValueError(f"poll interval ({poll_interval_s}s) cannot be larger than timeout ({timeout_s}s)")
        if poll_interval_s > idle_interval_s:
            raise ValueError(f"poll interval ({poll_interval_s}s) cannot be larger "
                             f"than idle interval ({idle_interval_s}s)")

        start_time_s = time.monotonic()
        idle_started_s = None
        prev = (0, 0)
        while True:
            now_s = time.monotonic()

            # Metrics retrieval
            metrics: dict = self.client.get_pipeline_stats(self.pipeline_name).get("global_metrics")
            total_input_records: int | None = metrics.get("total_input_records")
            total_processed_records: int | None = metrics.get("total_processed_records")
            if total_input_records is None:
                raise RuntimeError("total_input_records is missing from the pipeline metrics")
            if total_processed_records is None:
                raise RuntimeError("total_processed_records is missing from the pipeline metrics")

            # Idle check
            unchanged = prev[0] == total_input_records and prev[1] == total_processed_records
            equal = total_input_records == total_processed_records
            prev = (total_input_records, total_processed_records)
            if unchanged and equal:
                if idle_started_s is None:
                    idle_started_s = now_s
            else:
                idle_started_s = None
            if idle_started_s is not None and now_s - idle_started_s >= idle_interval_s:
                return

            # Timeout
            if now_s - start_time_s >= timeout_s:
                raise RuntimeError(f"waiting for idle reached timeout ({timeout_s}s)")
            time.sleep(poll_interval_s)

    def pause(self):
        """
        Pause the pipeline.
        """

        self.client.pause_pipeline(self.pipeline_name)

    def shutdown(self):
        """
        Shut down the pipeline.
        """

        for view_queue in self.views_tx:
            for view_name, queue in view_queue.items():
                # sends a message to the callback runner to stop listening
                queue.put(_CallbackRunnerInstruction.RanToCompletion)
                # block until the callback runner has been stopped
                queue.join()

        self.client.shutdown_pipeline(self.pipeline_name)

    def resume(self):
        """
        Resumes the pipeline.
        """

        self.client.start_pipeline(self.pipeline_name)

    def delete(self, delete_program: bool = True, delete_connectors: bool = False):
        """
        Delete the pipeline.

        :param delete_program: If True, also deletes the program associated with the pipeline. True by default.
        :param delete_connectors: If True, also deletes the connectors associated with the pipeline. False by default.
        """

        current_status = self.pipeline_status()

        if current_status == PipelineStatus.NOT_FOUND:
            raise RuntimeError("Attempting to delete a pipeline that hasn't been created yet")

        if current_status not in [PipelineStatus.SHUTDOWN, PipelineStatus.FAILED]:
            raise RuntimeError("Pipeline must be shutdown before deletion")

        self.client.delete_pipeline(self.pipeline_name)

        if delete_program:
            self.client.delete_program(self.program_name)

        if delete_connectors:
            for connector in self.input_connectors_buffer.values():
                for conn in connector:
                    self.client.delete_connector(conn.name)
            for connector in self.output_connectors_buffer.values():
                for conn in connector:
                    self.client.delete_connector(conn.name)

import time
import pandas
import re

from typing import Optional, Dict, Callable

from feldera.rest.pipeline import Pipeline
from typing_extensions import Self
from queue import Queue

from feldera.rest.errors import FelderaAPIError
from feldera import FelderaClient
from feldera.output_handler import OutputHandler
from feldera._callback_runner import CallbackRunner, _CallbackRunnerInstruction
from feldera._helpers import ensure_dataframe_has_columns
from feldera.runtime_config import Resources, RuntimeConfig
from feldera.enums import BuildMode, CompilationProfile, PipelineStatus
from feldera._helpers import chunk_dataframe


def _table_name_from_sql(ddl: str) -> str:
    return re.findall(r"[\w']+", ddl)[2]


class SQLContext:
    """
    .. _SQLContext:

    The SQLContext is the main entry point for the Feldera SQL API.
    Abstracts the interaction with the Feldera API and provides a high-level interface for SQL pipelines.

    :param name: The name of the pipeline.
    :param client: The :class:`.FelderaClient` instance to use.
    :param pipeline_description: The description of the pipeline.
    :param storage: Set `True` to use storage with this pipeline. Defaults to False.
    :param workers: The number of workers to use with this pipeline. Defaults to 8.
    :param resources: The :class:`.Resources` for the pipeline. Defaults to None.
    :param runtime_config: The :class:`.RuntimeConfig` for the pipeline. Defaults to None.
    :param compilation_profile: The compilation profile to use when compiling the program. Defaults to
        :class:`.CompilationProfile.OPTIMIZED`.
    """

    def __init__(
            self,
            name: str,
            client: FelderaClient,
            pipeline_description: str = None,
            storage: bool = False,
            workers: int = 8,
            resources: Resources = Resources(),
            runtime_config: RuntimeConfig = None,
            compilation_profile: CompilationProfile = CompilationProfile.OPTIMIZED
    ):
        self.ddl = ""
        self.build_mode: Optional[BuildMode] = None
        self.views_tx: list[Dict[str, Queue]] = []
        self.client: FelderaClient = client
        self.pipeline_name: str = name
        self.pipeline_description: str = pipeline_description or ""
        self.storage: bool = storage
        self.workers: int = workers
        self.resources: Resources = resources
        self.runtime_config: RuntimeConfig = runtime_config or RuntimeConfig(resources=self.resources)
        self.compilation_profile: CompilationProfile = compilation_profile

    def __setup_output_listeners(self):
        """
        Internal function used to set up the output listeners.

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
            return PipelineStatus.from_str(pipeline.deployment_status)

        except FelderaAPIError as err:
            if err.status_code == 404:
                return PipelineStatus.NOT_FOUND
            else:
                raise err

    def sql(self, sql: str):
        """
        Appends this SQL code to the pipeline DDL.


        :param sql: One or more SQL statements. Each statement must end with a semicolon.
        """

        self.ddl += sql.strip()
        self.ddl += "\n"

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

        pipeline = self.client.get_pipeline(self.pipeline_name)
        if table_name.lower() != "now" and table_name.lower() not in [tbl.name.lower() for tbl in pipeline.tables]:
            raise ValueError(f"Cannot push to table '{table_name}' as it is not registered yet")
        else:
            # consider validating the schema here
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

    def input_json(self, table_name: str, data: Dict | list, force: bool = False):
        """
        Push this JSON data to the specified table of the pipeline.

        :param table_name: The name of the table to push data into.
        :param data: The JSON encoded data to be pushed to the pipeline. The data should be in the form:
            `{'col1': 'val1', 'col2': 'val2'}` or `[{'col1': 'val1', 'col2': 'val2'}, {'col1': 'val1', 'col2': 'val2'}]`
        :param force: `True` to push data even if the pipeline is paused. `False` by default.
        """

        array = True if isinstance(data, list) else False
        self.client.push_to_pipeline(self.pipeline_name, table_name, "json", data, array=array, force=force)

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

        pipeline = Pipeline(
            self.pipeline_name,
            sql=self.ddl,
            description=self.pipeline_description,
            program_config={
                'profile': self.compilation_profile.value,
            },
            runtime_config=self.runtime_config.__dict__,
        )

        self.client.create_pipeline(pipeline)

        current_state = self.pipeline_status()
        if current_state not in [PipelineStatus.NOT_FOUND, PipelineStatus.SHUTDOWN]:
            raise RuntimeError(f"pipeline in state: {str(current_state.name)} cannot be started")

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

    def delete(self):
        """
        Deletes the pipeline.
        """

        self.client.delete_pipeline(self.pipeline_name)



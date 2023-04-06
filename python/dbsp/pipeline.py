import dbsp_api_client
import time
import sys

from typing import Dict, Any
from dbsp_api_client.client import Client
from dbsp_api_client.api.pipeline import pipeline_start
from dbsp_api_client.api.pipeline import pipeline_pause
from dbsp_api_client.api.pipeline import pipeline_shutdown
from dbsp_api_client.api.pipeline import pipeline_delete
from dbsp_api_client.api.pipeline import pipeline_metadata
from dbsp_api_client.api.pipeline import pipeline_status
from dbsp_api_client.models.shutdown_pipeline_request import ShutdownPipelineRequest
from dbsp.error import TimeoutException


class DBSPPipeline:
    """DBSP pipeline instance.
    """

    def __init__(self, config, api_client: Client, pipeline_id: int):
        self.api_client = api_client
        self.pipeline_id = pipeline_id
        self.config = config
        pipeline_start.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id).unwrap("Failed to start pipeline")

    def pause(self):
        """Pause pipeline.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        pipeline_pause.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id).unwrap("Failed to pause pipeline")

    def start(self):
        """Start paused pipeline.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        pipeline_start.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id).unwrap("Failed to start pipeline")

#    def shutdown(self):
#        """Terminate the execution of a pipeline.
#
#        Sends a termination request to the pipeline process.
#        Returns immediately, without waiting for the pipeline
#        to terminate (which can take several seconds).
#
#        The pipeline is not deleted from the server.
#
#        Raises:
#            httpx.TimeoutException: If the request takes longer than Client.timeout.
#            dbsp.DBSPServerError: If the DBSP server returns an error.
#        """
#        request = ShutdownPipelineRequest(pipeline_id = self.pipeline_id)
#        pipeline_shutdown.sync_detailed(client = self.api_client, json_body = request).unwrap("Failed to stut down pipeline")

    def delete(self):
        """Terminate and delete a pipeline.

        Shut down the pipeline if it is still running and delete it from
        the server.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        pipeline_delete.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id).unwrap("Failed to delete pipeline")
        self.config.pipeline_id = None

    def status(self) -> Dict[str, Any]:
        """Retrieve pipeline status and performance counters.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        status = pipeline_status.sync_detailed(
            client=self.api_client, pipeline_id=self.pipeline_id).unwrap("Failed to retrieve pipeline status")
        return status.additional_properties

    def wait(self, timeout: float = sys.maxsize):
        """Wait for the pipeline to process all inputs to completion.

        This method should only be used for pipelines configured with finite input streams, e.g., files.

        Raises:
            httpx.TimeoutException: If the DBSP server takes longer than Client.timeout to reply to a request.
            dbsp.DBSPServerError: If the DBSP server returns an error.
            dbsp.TimeoutException: If the pipeline does not terminate within 'timeout' seconds.
        """
        start = time.time()
        while time.time() - start < timeout:
            status = self.status()
            if status['global_metrics']['pipeline_complete'] == True:
                return
            time.sleep(0.5)
        raise TimeoutException(
            "Timeout waiting for the pipeline to complete after " + str(timeout) + "s")

    def metadata(self) -> Dict[str, Any]:
        """Retrieve pipeline metadata.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.
        """
        meta = pipeline_metadata.sync_detailed(client=self.api_client, pipeline_id=self.pipeline_id).unwrap(
            "Failed to retrieve pipeline metadata")
        return meta.additional_properties

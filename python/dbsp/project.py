import dbsp_api_client

from dbsp_api_client.models.compile_project_request import CompileProjectRequest
from dbsp_api_client.api.project import project_status
from dbsp_api_client.api.project import compile_project
from dbsp.error import CompilationException
import time
import sys
from typing import Union, Dict, Any

class DBSPProject:
    """DBSP project

    A project is a SQL script with a non-unique name and a unique ID
    attached to it.  The client can add, remove, modify, and compile projects.
    Compilation includes running the SQL-to-DBSP compiler followed by the Rust
    compiler.
    """

    def __init__(self, api_client, project_id, project_version):
        self.api_client = api_client
        self.project_id = project_id
        self.project_version = project_version

    def compile(self, *, timeout: float = sys.maxsize):
        """Compile the project

        Queue project for compilation and wait for compilation to complete.

        Args:
            timeout (float): Maximal amount of time to wait for compilation to complete.

        Raises:
            httpx.TimeoutException: If the DBSP server takes too long to respond to a request.
                Note that compilation timeout is signalled by the dbsp.TimeoutException, while this exception
                indicates that the server is slow to respond to an HTTP request.
            dbsp.CompilationException: If the project fails to compile.
            dbsp.TimeoutException: If the project takes too long to compile.
            dbsp.DBSPServerError: If the DBSP server returns an error while queueing the project or probing project status.
        """

        body = CompileProjectRequest(
            project_id=self.project_id,
            version=self.project_version,
        )
        # Queue project for compilation.
        compile_project.sync_detailed(client = self.api_client, json_body=body).unwrap("Failed to queue project for compilation")

        start = time.time()
        while time.time() - start < timeout:
            status = self.status()
            if status != 'CompilingSql' and status != 'CompilingRust' and status != 'Pending':
                if status == 'Success':
                    return
                else:
                    raise CompilationException(str(status))
            time.sleep(0.5)

        raise TimeoutException("Timeout waiting for the project to compile after " + str(timeout) + "s")

    # TODO: Convert return type to something more user-friendly.
    def status(self) -> Union[Dict[str, Any], str]:
        """Returns project compilation status.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            Union[Dict[str, Any], str]
        """
        response = project_status.sync_detailed(
                client = self.api_client,
                project_id = self.project_id).unwrap("Failed to retrieve project status")

        # if api_response.body['version'] != self.project_version:
        #    raise RuntimeError(
        #            "Project modified on the server.  Expected version: " + self.project_version + ". ") from e

        return response.status

import dbsp_api_client

from dbsp_api_client.models.compile_program_request import CompileProgramRequest
from dbsp_api_client.api.programs import get_program
from dbsp_api_client.api.programs import compile_program
from dbsp.error import CompilationException
import time
import sys
from typing import Union, Dict, Any

class DBSPProgram:
    """DBSP program

    A program is a SQL script with a non-unique name and a unique ID
    attached to it.  The client can add, remove, modify, and compile programs.
    Compilation includes running the SQL-to-DBSP compiler followed by the Rust
    compiler.
    """

    def __init__(self, api_client, program_id, program_version):
        self.api_client = api_client
        self.program_id = program_id
        self.program_version = program_version

    def compile(self, *, timeout: float = sys.maxsize):
        """Compile the program

        Queue program for compilation and wait for compilation to complete.

        Args:
            timeout (float): Maximal amount of time to wait for compilation to complete.

        Raises:
            httpx.TimeoutException: If the DBSP server takes too long to respond to a request.
                Note that compilation timeout is signalled by the dbsp.TimeoutException, while this exception
                indicates that the server is slow to respond to an HTTP request.
            dbsp.CompilationException: If the program fails to compile.
            dbsp.TimeoutException: If the program takes too long to compile.
            dbsp.DBSPServerError: If the DBSP server returns an error while queueing the program or probing program status.
        """

        body = CompileProgramRequest(
            version=self.program_version,
        )
        # Queue program for compilation.
        compile_program.sync_detailed(client = self.api_client, program_id = self.program_id, json_body=body).unwrap("Failed to queue program for compilation")

        start = time.time()
        while time.time() - start < timeout:
            status = self.status()
            if status != 'CompilingSql' and status != 'CompilingRust' and status != 'Pending':
                if status == 'Success':
                    return
                else:
                    raise CompilationException(str(status))
            time.sleep(0.5)

        raise TimeoutException("Timeout waiting for the program to compile after " + str(timeout) + "s")

    # TODO: Convert return type to something more user-friendly.
    def status(self) -> Union[Dict[str, Any], str]:
        """Returns program compilation status.

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            Union[Dict[str, Any], str]
        """
        response = get_program.sync_detailed(
                client = self.api_client,
                program_id = self.program_id).unwrap("Failed to retrieve program status")

        # if api_response.body['version'] != self.program_version:
        #    raise RuntimeError(
        #            "Project modified on the server.  Expected version: " + self.program_version + ". ") from e

        return response.status

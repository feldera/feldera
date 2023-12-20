import feldera_api_client

from feldera_api_client.models.new_program_request import NewProgramRequest
from feldera_api_client.models.update_program_request import UpdateProgramRequest
from feldera_api_client.api.programs import get_programs
from feldera_api_client.api.programs import new_program
from feldera_api_client.api.programs import update_program
from dbsp.program import DBSPProgram
from http import HTTPStatus


class DBSPConnection:
    """DBSP server connection.

    Args:
        url (str): URL of the DBSP server.
    """

    def __init__(self, url):
        self.api_client = feldera_api_client.Client(base_url=url, timeout=20.0)

        get_programs.sync_detailed(client=self.api_client).unwrap(
            "Failed to fetch program list from the DBSP server"
        )

    def create_program(
        self, *, name: str, sql_code: str, description: str = ""
    ) -> DBSPProgram:
        """Create a new program.

        Args:
            name (str): Project name
            sql_code (str): SQL code for the program
            description (str): Project description

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProgram
        """

        return self.create_program_inner(
            name=name, sql_code=sql_code, description=description, replace=False
        )

    def create_or_replace_program(
        self, *, name: str, sql_code: str, description: str = ""
    ) -> DBSPProgram:
        """Create a new program overwriting existing program with the same name, if any.

        If a program with the same name already exists, all pipelines associated
        with that program and the program itself will be deleted.

        Args:
            name (str): Project name
            sql_code (str): SQL code for the program
            description (str): Project description

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProgram
        """

        return self.create_program_inner(
            name=name, sql_code=sql_code, description=description
        )

    def create_program_inner(self, *, name: str, sql_code: str, description: str):
        # Check if a program with `name` exists
        resp = get_programs.sync_detailed(client=self.api_client, name=name)
        if resp.status_code == HTTPStatus.OK:
            program_id = resp.unwrap("Failed to unwrap program %s" % (name))[
                0
            ].program_id
            # Update existing program
            request = UpdateProgramRequest(
                name=name, code=sql_code, description=description
            )
            program_response = update_program.sync_detailed(
                client=self.api_client, program_id=program_id, json_body=request
            ).unwrap("Failed to update the program")
        else:
            # Create a new one instead
            request = NewProgramRequest(
                name=name, code=sql_code, description=description
            )
            program_response = new_program.sync_detailed(
                client=self.api_client, json_body=request
            ).unwrap("Failed to create a program")
            program_id = program_response.program_id

        return DBSPProgram(
            api_client=self.api_client,
            program_id=program_id,
            program_name=name,
            program_version=program_response.version,
        )

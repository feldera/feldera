import dbsp_api_client

from dbsp_api_client.models.new_project_request import NewProjectRequest
from dbsp_api_client.api.project import list_projects
from dbsp_api_client.api.project import new_project
from dbsp.project import DBSPProject

class DBSPConnection:
    """DBSP server connection.

    Args:
        url (str): URL of the DBSP server.
    """

    def __init__(self, url="http://localhost:8080"):
        self.api_client = dbsp_api_client.Client(
                base_url = url,
                timeout = 20.0)

        list_projects.sync_detailed(client = self.api_client).unwrap("Failed to fetch project list from the DBSP server")

    def create_project(self, *, name: str, sql_code: str, description: str = '') -> DBSPProject:
        """Create a new project.

        Args:
            name (str): Project name
            sql_code (str): SQL code for the project
            description (str): Project description

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProject
        """

        return self.create_project_inner(name = name, sql_code = sql_code, description = description, replace = False)

    def create_or_replace_project(self, *, name: str, sql_code: str, description: str = '') -> DBSPProject:
        """Create a new project overwriting existing project with the same name, if any.

        If a project with the same name already exists, all pipelines associated
        with that project and the project itself will be deleted.

        Args:
            name (str): Project name
            sql_code (str): SQL code for the project
            description (str): Project description

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProject
        """

        return self.create_project_inner(name = name, sql_code = sql_code, description = description, replace = True)

    def create_project_inner(self, *, name: str, sql_code: str, description: str, replace: bool):
        request = NewProjectRequest(name=name, overwrite_existing = replace, code=sql_code, description='')

        new_project_response = new_project.sync_detailed(client = self.api_client, json_body=request).unwrap("Failed to create a project")

        return DBSPProject(
            api_client=self.api_client,
            project_id=new_project_response.project_id,
            project_version=new_project_response.version)

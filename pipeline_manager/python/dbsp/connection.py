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

    def new_project(self, *, name: str = "<noname>", sql_code: str) -> DBSPProject:
        """Create a new project.

        Args:
            name (str): Project name
            sql_code (str): SQL code for the project

        Raises:
            httpx.TimeoutException: If the request takes longer than Client.timeout.
            dbsp.DBSPServerError: If the DBSP server returns an error.

        Returns:
            DBSPProject
        """

        request = NewProjectRequest(code=sql_code, name=name, description='')

        new_project_response = new_project.sync_detailed(client = self.api_client, json_body=request).unwrap("Failed to create a project")

        return DBSPProject(
            api_client=self.api_client,
            project_id=new_project_response.project_id,
            project_version=new_project_response.version)

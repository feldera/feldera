import uuid
from typing import Optional


class AttachedConnector:
    """
    A connector that is attached to a pipeline.
    """

    def __init__(self, connector_name: str, relation_name: str, is_input: bool, name: Optional[str] = None):
        """
        :param connector_name: The name of the connector.
        :param relation_name: The name of the relation / table / view to attach to.
        :param is_input: True if the connector is to be used for input.
        :param name: A unique name for this connector instance.
        """

        self.name: str = name or str(uuid.uuid4())
        self.is_input: bool = is_input
        self.connector_name: str = connector_name
        self.relation_name: str = relation_name

    def to_json(self):
        return {
            "name": self.name,
            "is_input": self.is_input,
            "connector_name": self.connector_name,
            "relation_name": self.relation_name
        }
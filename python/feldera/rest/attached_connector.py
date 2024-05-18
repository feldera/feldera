import uuid
from typing import Optional


class AttachedConnector:
    """
    A connector that is attached to a pipeline.
    """

    name: str
    is_input: bool
    connector_name: str
    relation_name: str

    def __init__(self, connector_name: str, relation_name: str, is_input: bool, name: Optional[str] = None):
        self.name = name or str(uuid.uuid4())
        self.is_input = is_input
        self.connector_name = connector_name
        self.relation_name = relation_name

    def to_json(self):
        return {
            "name": self.name,
            "is_input": self.is_input,
            "connector_name": self.connector_name,
            "relation_name": self.relation_name
        }
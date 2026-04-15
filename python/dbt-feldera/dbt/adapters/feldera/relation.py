from dataclasses import dataclass, field

from dbt_common.dataclass_schema import StrEnum

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import (
    ComponentName,
    Policy,
)


class FelderaRelationType(StrEnum):
    """Feldera-specific relation types."""

    Table = "table"
    View = "view"
    MaterializedView = "materialized_view"


@dataclass(frozen=True, eq=False, repr=False)
class FelderaRelation(BaseRelation):
    """
    Represents a relation (table or view) in a Feldera pipeline.

    In Feldera's model:
    - database = Feldera instance (logical grouping)
    - schema = pipeline name
    - identifier = table or view name within the pipeline

    Relations render as bare identifiers since all tables and views
    share a single pipeline SQL program namespace.
    """

    quote_character: str = '"'

    # Only include the identifier in rendered SQL (no db/schema prefix)
    include_policy: Policy = field(default_factory=lambda: Policy(database=False, schema=False, identifier=True))

    @classmethod
    def get_relation_type(cls) -> type:
        return FelderaRelationType

    def render(self) -> str:
        """
        Render the relation as a bare identifier.

        Feldera pipelines currently use a flat namespace — no database or
        schema qualification is needed. Only the identifier matters.

        This may change in the future if Feldera adds multi-schema support.
        """
        if self.identifier:
            return self.quoted(self.identifier)
        return ""

    def quoted(self, identifier: str) -> str:
        """
        Quote an identifier using double quotes (Calcite SQL standard).

        :param identifier: The identifier to quote.
        :return: The quoted identifier string.
        """
        if self.quote_policy.get(ComponentName.Identifier):
            return f"{self.quote_character}{identifier}{self.quote_character}"
        return identifier

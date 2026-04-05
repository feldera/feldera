from dbt.adapters.base.column import Column


class FelderaColumn(Column):
    """
    Represents a column in a Feldera table or view.

    Maps Feldera's SQL type system (Calcite-based) to dbt's column model.
    """

    # Maps Feldera SQL types to dbt's canonical type labels for data-type
    # aliasing.
    #
    # dbt's ``Column.translate_type()`` uses this dict to normalize
    # database-native type names into a small set of canonical labels
    # (e.g. ``"integer"``, ``"text"``, ``"float"``) in the dbt project manifest.
    #
    # The mapping is consumed in two places:
    #
    # 1. **Model contract enforcement** — When a model declares
    #    ``contract.alias_types: true`` (the default), dbt translates every
    #    column's ``data_type`` through this dict before comparing the
    #    declared schema against the actual schema returned by the database.
    #    This allows a contract to declare ``INTEGER`` even though Feldera
    #    reports ``INT``, because both resolve to ``"integer"``.
    #    See: https://docs.getdbt.com/reference/resource-configs/contract#data-type-aliasing
    #
    # 2. **Column introspection** — ``Column.create()`` calls
    #    ``translate_type()`` when building column objects from query
    #    results, so that helper predicates like ``is_string()`` and
    #    ``is_number()`` work regardless of the exact native type name.
    #    See: https://docs.getdbt.com/reference/dbt-jinja-functions/builtins#column
    #
    # Each key is a Feldera SQL type name (upper-cased); each value is the
    # canonical label that dbt and this adapter use for comparison and
    # classification.  Types that have no natural numeric or temporal label
    # (e.g. ARRAY, MAP, ROW) are mapped to ``"text"`` as a safe fallback.
    #
    TYPE_LABELS = {
        "BOOLEAN": "boolean",
        "TINYINT": "integer",
        "SMALLINT": "integer",
        "INTEGER": "integer",
        "INT": "integer",
        "BIGINT": "integer",
        "REAL": "float",
        "FLOAT": "float",
        "DOUBLE": "float",
        "DECIMAL": "numeric",
        "NUMERIC": "numeric",
        "VARCHAR": "text",
        "STRING": "text",
        "CHAR": "text",
        "TEXT": "text",
        "BINARY": "text",
        "VARBINARY": "text",
        "DATE": "date",
        "TIME": "time",
        "TIMESTAMP": "datetime",
        "INTERVAL": "text",
        "ARRAY": "text",
        "MAP": "text",
        "ROW": "text",
        "VARIANT": "text",
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        """
        Translate a Feldera SQL type to its canonical dbt type label.

        Extracts the base type name (stripping parameters like precision and
        scale) via sqlglot, then looks it up in :attr:`TYPE_LABELS`.  Returns
        ``"text"`` for any unrecognized type.

        :param dtype: A Feldera SQL data type string (e.g. ``"DECIMAL(10,2)"``).
        :return: A canonical label such as ``"integer"``, ``"float"``, ``"text"``.
        """
        from dbt.adapters.feldera.sqlglot_parser import parser

        base_type = parser.sql_type_base_name(dtype)
        return cls.TYPE_LABELS.get(base_type, "text")

    @classmethod
    def from_feldera_field(cls, field: dict) -> "FelderaColumn":
        """
        Create a FelderaColumn from a Feldera pipeline schema field definition.

        :param field: A dict with 'name' and 'columntype' keys from the pipeline schema.
        :return: A FelderaColumn instance.
        """
        name = field.get("name", "")
        column_type = field.get("columntype", {})
        dtype = column_type.get("type", "VARCHAR") if isinstance(column_type, dict) else str(column_type)
        return cls(column=name, dtype=dtype)

    def is_string(self) -> bool:
        """Return True if this column is a string/text type."""
        return self.translate_type(self.dtype) == "text"

    def is_integer(self) -> bool:
        """Return True if this column is an integer type."""
        return self.translate_type(self.dtype) == "integer"

    def is_number(self) -> bool:
        """Return True if this column is any numeric type."""
        return any([self.is_integer(), self.is_numeric(), self.is_float()])

    def is_float(self) -> bool:
        """Return True if this column is an IEEE floating-point type (REAL, FLOAT, DOUBLE)."""
        return self.translate_type(self.dtype) == "float"

    def is_numeric(self) -> bool:
        """Return True if this column is a fixed-precision numeric type (DECIMAL, NUMERIC)."""
        return self.translate_type(self.dtype) == "numeric"

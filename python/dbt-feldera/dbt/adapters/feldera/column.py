from dbt.adapters.base.column import Column


class FelderaColumn(Column):
    """
    Represents a column in a Feldera table or view.

    Maps Feldera's SQL type system (Calcite-based) to dbt's column model.
    """

    # Feldera SQL type mappings to dbt categories
    TYPE_LABELS = {
        "BOOLEAN": "boolean",
        "TINYINT": "integer",
        "SMALLINT": "integer",
        "INTEGER": "integer",
        "INT": "integer",
        "BIGINT": "integer",
        "REAL": "number",
        "FLOAT": "number",
        "DOUBLE": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
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
        Translate a Feldera SQL type to its dbt category label.

        :param dtype: The Feldera SQL data type string.
        :return: The dbt type category (e.g., 'text', 'integer', 'number').
        """
        base_type = dtype.split("(")[0].strip().upper()
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
        """Return True if this column is a numeric type."""
        return self.translate_type(self.dtype) in ("integer", "number")

    def is_float(self) -> bool:
        """Return True if this column is a floating-point type."""
        return self.translate_type(self.dtype) == "number"

    def string_size(self) -> int:
        """Return the maximum length for string types."""
        return 65535

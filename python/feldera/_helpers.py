import uuid

import pandas as pd
from decimal import Decimal


def sql_type_to_pandas_type(sql_type: str):
    """
    Converts a SQL type to a pandas type.
    """

    match sql_type.upper():
        case "UUID":
            return None
        case "BOOLEAN":
            return "boolean"
        case "TINYINT":
            return "Int8"
        case "SMALLINT":
            return "Int16"
        case "INTEGER":
            return "Int32"
        case "BIGINT":
            return "Int64"
        case "REAL":
            return "Float32"
        case "DOUBLE":
            return "Float64"
        case "DECIMAL":
            return None
        case "CHAR":
            return "str"
        case "VARCHAR":
            return "str"
        case "DATE" | "TIMESTAMP":
            return "datetime64[ns]"
        case "TIME" | "INTERVAL":
            return "timedelta64[ns]"
        case "ARRAY":
            return None
        case "NULL":
            return None
        case "BINARY" | "VARBINARY":
            return None
        case "STRUCT" | "MAP":
            return None


def ensure_dataframe_has_columns(df: pd.DataFrame):
    """
    Ensures that the DataFrame has column names set.
    """

    if [v for v in range(df.shape[1])] == list(df.columns):
        raise ValueError(
            """
            DataFrame has no column names set.
            Input DataFrame must have column names set and they must be consistent with the columns in the input table.
            """
        )


def dataframe_from_response(buffer: list[list[dict]], schema: dict):
    """
    Converts the response from Feldera to a pandas DataFrame.
    """

    pd_schema = {}

    decimal_col = []
    uuid_col = []

    for column in schema["fields"]:
        column_name = column["name"]
        if not column["case_sensitive"]:
            column_name = column_name.lower()
        column_type = column["columntype"]["type"]
        if column_type == "DECIMAL":
            decimal_col.append(column_name)
        elif column_type == "UUID":
            uuid_col.append(column_name)

        pd_schema[column_name] = sql_type_to_pandas_type(column_type)

    data = [
        {**item["insert"], "insert_delete": 1}
        if "insert" in item
        else {**item["delete"], "insert_delete": -1}
        for sublist in buffer
        for item in sublist
    ]

    if len(decimal_col) != 0:
        for datum in data:
            for col in decimal_col:
                if datum[col] is not None:
                    datum[col] = Decimal(datum[col])

    if len(uuid_col) != 0:
        for datum in data:
            for col in uuid_col:
                if datum[col] is not None:
                    datum[col] = uuid.UUID(datum[col])

    df = pd.DataFrame(data)
    df = df.astype(pd_schema)

    return df


def chunk_dataframe(df, chunk_size=1000):
    """
    Yield successive n-sized chunks from the given dataframe.
    """

    for i in range(0, len(df), chunk_size):
        yield df.iloc[i : i + chunk_size]

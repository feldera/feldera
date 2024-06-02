import pandas as pd
from feldera.formats import JSONFormat, CSVFormat


def dataframe_from_response(buffer: list[list[dict]]):
    """
    Converts the response from Feldera to a pandas DataFrame.
    """
    return pd.DataFrame([
        {**item['insert'], 'insert_delete': 1} if 'insert' in item else {**item['delete'], 'insert_delete': -1}
        for sublist in buffer for item in sublist
    ])


def validate_connector_input_format(fmt: JSONFormat | CSVFormat):
    if not isinstance(fmt, JSONFormat) and not isinstance(fmt, CSVFormat):
        raise ValueError("format must be JSONFormat or CSVFormat")

    if isinstance(fmt, JSONFormat) and fmt.config.get("update_format") is None:
        raise ValueError("update_format not set in the format config; consider using: .with_update_format()")

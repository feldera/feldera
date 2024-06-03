import pandas as pd


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


def dataframe_from_response(buffer: list[list[dict]]):
    """
    Converts the response from Feldera to a pandas DataFrame.
    """
    return pd.DataFrame([
        {**item['insert'], 'insert_delete': 1} if 'insert' in item else {**item['delete'], 'insert_delete': -1}
        for sublist in buffer for item in sublist
    ])

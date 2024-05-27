import pandas as pd


def dataframe_from_response(buffer: list[list[dict]]):
    """
    Converts the response from Feldera to a pandas DataFrame.
    """
    return pd.DataFrame([
        {**item['insert'], 'insert_delete': 1} if 'insert' in item else {**item['delete'], 'insert_delete': -1}
        for sublist in buffer for item in sublist
    ])

"""
This package provides a simple interface for working with feldera via python.

You will need to first import `FelderaClient`, and give it the URL, api_key (if required)
of the feldera instance you want to connect to.

You can then use `SQLContext` to set up the source (something that provides data to feldera) and the sink
(something that receives data from feldera).

You use SQL to write the query that will be executed on the data coming from the source. This query creates a
"view" in feldera. You can forward the data from this view to the sink of your choice, or stack views on
top of each other.

Example usage:

.. highlight:: python
.. code-block:: python

    from feldera import FelderaClient, SQLContext, SQLSchema
    import pandas as pd

    # Create a client
    client = FelderaClient("https://try.feldera.com", api_key="YOUR_API_KEY")

    # Create a SQLContext
    sql = SQLContext("notebook", client).get_or_create()

    TBL_NAMES = ["students", "grades"]
    view_name = "average_scores"

    df_students = pd.read_csv("students.csv")
    df_grades = pd.read_csv("grades.csv")

    # register an input table
    # tables receive data from the source, therefore they need a schema
    sql.register_table(TBL_NAMES[0], SQLSchema({"name": "STRING", "id": "INT"}))

    sql.register_table(TBL_NAMES[1], SQLSchema({
        "student_id": "INT",
        "science": "INT",
        "maths": "INT",
        "art": "INT"
    }))

    # here, we provide a query, that gets registered as a view in feldera
    # this query will be executed on the data in the table
    query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC"
    sql.register_view(view_name, query)

    # connect the source (a pandas Dataframe in this case) to the tables
    sql.connect_source_pandas(TBL_NAMES[0], df_students)
    sql.connect_source_pandas(TBL_NAMES[1], df_grades)

    # listen for the output of the view here in the notebook
    # you do not need to call this if you are forwarding the data to a sink
    out = sql.listen(view_name)

    # run this to completion
    # note that if the source is a stream, this will run indefinitely
    sql.run_to_completion()

    # finally, convert the output to a pandas Dataframe
    df = out.to_pandas()

    # see the result
    print(df)
"""

from feldera.rest.client import Client as FelderaClient
from feldera.sql_context import SQLContext
from feldera.sql_schema import SQLSchema

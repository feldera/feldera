"""
This package provides a simple interface for working with Feldera from Python.

* Create a `FelderaClient` object and give it the URL, and the API key (if required)
  of the Feldera instance you want to connect to.

* Create an `SQLContext`_ object and add SQL tables, which specify inputs to Feldera, and views, which define
  queries to run on these inputs.
* Set up sources, which provide data to tables, and sinks, which consume data from views.
* Call `start`_ on the `SQLContext`_ object to create a Feldera pipeline to continuously ingest data from sources into
  SQL tables, incrementally evaluate SQL views, and send outputs to sinks. Alternatively,
  call `run_to_completion`_ to create a pipeline that processes all inputs to completion and exits.

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

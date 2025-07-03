Pandas Compatibility
====================

Feldera tries to be compatible with the Pandas as much as possible.
However, some types in SQL have limited support in Pandas.

Columns with the following SQL types will be converted to the corresponding Pandas types:

.. csv-table::
   :header: "SQL Type", "Pandas Type"

    "BOOLEAN", "bool"
    "TINYINT", "Int8"
    "SMALLINT", "Int16"
    "INTEGER", "Int32"
    "BIGINT", "Int64"
    "REAL", "Float32"
    "DOUBLE", "Float64"
    "DECIMAL", "decimal.Decimal"
    "CHAR", "str"
    "VARCHAR", "str"
    "DATE", "datetime64[ns]"
    "TIMESTAMP", "datetime64[ns]"
    "TIME", "timedelta64[ns]"
    "INTERVAL", "timedelta64[ns]"
    "ARRAY", "object"
    "BINARY", "object"
    "VARBINARY", "object"
    "STRUCT", "object"
    "MAP", "object"


.. note::
    Please note that the "object" type in Pandas is dynamic and can hold any type of data.
    Therefore, the representation of primitive types in arrays, binary, struct, and map types may be different to their
    representation as a standalone column.

Using Pandas DataFrames as Input / Output
*******************************************

You can use :meth:`.Pipeline.input_pandas` to insert records from a
DataFrame to a Feldera table.

Use :meth:`.Pipeline.listen` to subscribe to updates to a view in the form of a stream of DataFrames.
To ensure all data is received start listening before calling
:meth:`.Pipeline.start`.

.. highlight:: python
.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder
    import pandas as pd

    sql = f"""
    CREATE TABLE students (
        name STRING,
        id INT
    );

    CREATE TABLE grades (
        student_id INT,
        science INT,
        maths INT,
        art INT
    );

    CREATE VIEW average_scores AS SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC;
    """

    # Create a client
    client = FelderaClient("https://try.feldera.com", api_key="YOUR_API_KEY")
    pipeline = PipelineBuilder(client, name="notebook", sql=sql).create_or_replace()

    df_students = pd.read_csv('students.csv')
    df_grades = pd.read_csv('grades.csv')

    # listen for the output of the view here in the notebook
    # you do not need to call this if you are forwarding the data to a sink
    out = pipeline.listen("average_scores")

    pipeline.start()
    pipeline.input_pandas("students", df_students)
    pipeline.input_pandas("grades", df_grades)

    # wait for the pipeline to complete
    # note that if the source is a stream, this will run indefinitely
    pipeline.wait_for_completion(True)
    df = out.to_pandas()

    # see the result
    print(df)

    # clear the storage and delete the pipeline
    pipeline.delete(True)

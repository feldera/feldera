Examples
========

Using Pandas DataFrames as Input / Output
*******************************************


You can use :meth:`.Pipeline.input_pandas` to input a
DataFrame to a Feldera table as the data source.

To listen for response from Feldera, in the form of DataFrames
call :meth:`.Pipeline.listen`.
To ensure all data is received start listening before calling
:meth:`.Pipeline.start`.

.. highlight:: python
.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder
    import pandas as pd

    TBL_NAMES = ['students', 'grades']
    view_name = "average_scores"

    sql = f"""
    CREATE TABLE {TBL_NAMES[0]} (
        name STRING,
        id INT
    );

    CREATE TABLE {TBL_NAMES[1]} (
        student_id INT,
        science INT,
        maths INT,
        art INT
    );

    CREATE VIEW {view_name} AS SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC;
    """

    # Create a client
    client = FelderaClient("https://try.feldera.com", api_key="YOUR_API_KEY")
    pipeline = PipelineBuilder(client).with_name("notebook").with_sql(sql).create_or_replace()

    df_students = pd.read_csv('students.csv')
    df_grades = pd.read_csv('grades.csv')

    # listen for the output of the view here in the notebook
    # you do not need to call this if you are forwarding the data to a sink
    out = pipeline.listen("average_scores")

    pipeline.start()
    pipeline.input_pandas(TBL_NAMES[0], df_students)
    pipeline.input_pandas(TBL_NAMES[1], df_grades)

    # wait for the pipeline to complete
    # note that if the source is a stream, this will run indefinitely
    pipeline.wait_for_completion(True)
    df = out.to_pandas()

    # see the result
    print(df)

    pipeline.delete()

Using Other Data Sources / Sinks
**********************************

To connect Feldera to other data sources or sinks, you can use specify them in the SQL code itself.
Refer to the connector documentation at: https://github.com/feldera/feldera/tree/main/docs/connectors

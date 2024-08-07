Examples
========

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

    pipeline.delete()

Using Other Data Sources / Sinks
**********************************

To connect Feldera to other data sources or sinks, you can specify them in the SQL code.
Refer to the connector documentation at: https://github.com/feldera/feldera/tree/main/docs/connectors

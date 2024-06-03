Examples
========

Pandas
*******


Working wth pandas DataFrames in Feldera is fairly straight forward. 
You can use :meth:`.SQLContext.connect_source_pandas` to connect a 
DataFrame to a feldera table as the data source. 

To listen for response from feldera, in the form of DataFrames, it is necessary
to to call :meth:`.SQLContext.listen` before you call 
:meth:`.SQLContext.start` or :meth:`.SQLContext.run_to_completion`.

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


Kafka
******

To setup Kafka as the source use :meth:`.SQLContext.connect_source_kafka` and as the sink use
:meth:`.SQLContext.connect_sink_kafka`.

Both of these methods require a ``config`` which is a dictionary, and ``fmt`` which is a
`data format configuration <https://www.feldera.com/docs/api/json>`_ that is either a
:class:`.JSONFormat` or :class:`.CSVFormat`.

The input config looks like the following:

.. highlight:: python
.. code-block:: python

    source_config = {
        "topics": [INPUT_TOPIC],
        "bootstrap.servers": KAFKA_SERVER_URL,
        "auto.offset.reset": "earliest",
    }

Here,

- ``topics`` is a list of Kafka topics to subscribe to for input data.
- ``bootstrap.servers`` is the ``host:port`` of the Kafka server.
- Similarly, other
  `relevant options supported by librdkafka <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>`_
  can also be set here, like: ``auto.offset.reset``

More on Kafka as an input connector at: https://www.feldera.com/docs/connectors/sources/kafka

Similarly, the output config looks like the following:

.. highlight:: python
.. code-block:: python

    sink_config = {
        "topic": OUTPUT_TOPIC,
        "bootstrap.servers": PIPELINE_TO_KAFKA_SERVER,
        "auto.offset.reset": "earliest",
    }

Here the only notable difference is:

- ``topic`` is the name of the topic to write the output data to.

More on Kafka as the output connector at: https://www.feldera.com/docs/connectors/sinks/kafka

.. warning::
    Kafka is a streaming data source, therefore running: :meth:`.SQLContext.run_to_completion` will run forever.

.. highlight:: python
.. code-block:: python

    from feldera import SQLContext, SQLSchema
    from feldera.formats import JSONFormat, JSONUpdateFormat

    TABLE_NAME = "example"
    VIEW_NAME = "example_count"
    KAFKA_SERVER = "localhost:9092"

    sql = SQLContext('kafka', 'http://localhost:8080').get_or_create()
    sql.register_table(TABLE_NAME, SQLSchema({"id": "INT NOT NULL PRIMARY KEY"}))
    sql.register_view(VIEW_NAME, f"SELECT COUNT(*) as num_rows FROM {TABLE_NAME}")

    source_config = {
        "topics": ["example_topic"],
        "bootstrap.servers": KAFKA_SERVER,
        "auto.offset.reset": "earliest",
    }

    sink_config = {
        "topic": "example_topic_out",
        "bootstrap.servers": KAFKA_SERVER,
        "auto.offset.reset": "earliest",
    }

    # Data format configuration
    format = JSONFormat().with_update_format(JSONUpdateFormat.InsertDelete).with_array(False)

    sql.connect_source_kafka(TABLE_NAME, "kafka_conn_in", source_config, format)
    sql.connect_sink_kafka(VIEW_NAME, "kafka_conn_out", sink_config, format)

    out = sql.listen(VIEW_NAME)
    sql.start()
    time.sleep(10)
    sql.shutdown()
    df = out.to_pandas()


HTTP GET
*********


Feldera can ingest data from a user-provided URL into a SQL table.
The file is fetched using HTTP with the GET method.

More on the HTTP GET connector at: https://www.feldera.com/docs/connectors/sources/http-get

.. note::
    The JSON used as input for Feldera should be in
    `newline-delimited JSON (NDJSON) format <https://www.feldera.com/docs/api/json/#encoding-multiple-changes>`_.


.. highlight:: python
.. code-block:: python

    from feldera import SQLContext, SQLSchema
    from feldera.formats import JSONFormat, JSONUpdateFormat

    sql = SQLContext("test_http_get", TEST_CLIENT).get_or_create()

    TBL_NAME = "items"
    VIEW_NAME = "s"

    sql.register_table(TBL_NAME, SQLSchema({"id": "INT", "name": "STRING"}))

    sql.register_view(VIEW_NAME, f"SELECT * FROM {TBL_NAME}")

    path = "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"

    fmt = JSONFormat().with_update_format(JSONUpdateFormat.InsertDelete).with_array(False)
    sql.connect_source_url(TBL_NAME, "part", path, fmt)

    out = sql.listen(VIEW_NAME)

    sql.run_to_completion()

    df = out.to_pandas()


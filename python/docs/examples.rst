Examples
========

Using Pandas DataFrames as Input / Output
*******************************************


You can use :meth:`.SQLContext.input_pandas` to connect a
DataFrame to a feldera table as the data source.

To listen for response from feldera, in the form of DataFrames
call :meth:`.SQLContext.listen`.
To ensure all data is received start listening before calling
:meth:`.SQLContext.start`.

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

    # create a table
    # tables receive data from the source, therefore they need a schema
    sql.sql(f"CREATE TABLE {TBL_NAMES[0]} (name STRING, id INT);")
    sql.sql(f"""
    CREATE TABLE {TBL_NAMES[1]} (
        student_id INT,
        science INT,
        maths INT,
        art INT
    );
    """)

    # here we create a view on the input data
    query = f"SELECT name, ((science + maths + art) / 3) as average FROM {TBL_NAMES[0]} JOIN {TBL_NAMES[1]} on id = student_id ORDER BY average DESC;"
    sql.sql(f"CREATE VIEW {view_name} AS {query}")

    # listen for the output of the view here in the notebook
    # you do not need to call this if you are forwarding the data to a sink
    out = sql.listen(view_name)

    # start the pipeline
    sql.start()

    # connect the source (a pandas Dataframe in this case) to the tables
    sql.input_pandas(TBL_NAMES[0], df_students)
    sql.input_pandas(TBL_NAMES[1], df_grades)

    # wait for the pipeline to complete
    # note that if the source is a stream, this will run indefinitely
    sql.wait_for_completion(shutdown=True)

    # finally, convert the output to a pandas Dataframe
    df = out.to_pandas()

    # see the result
    print(df)


Using Kafka as Data Source / Sink
***********************************

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
    Kafka is a streaming data source, therefore running: :meth:`.SQLContext.wait_for_completion` will block forever.

Creating a Kafka data source / sink:

.. highlight:: python
.. code-block:: python

    TABLE_NAME = "example"
    VIEW_NAME = "example_count"

    sql = SQLContext('kafka_test', TEST_CLIENT).get_or_create()

    sql.sql(f"""
    CREATE TABLE {TABLE_NAME} (id INT NOT NULL PRIMARY KEY)
    WITH (
        'connectors' = '[
            {{
                "name": "kafka-2",
                "transport": {{
                    "name": "kafka_input",
                    "config": {{
                        "bootstrap.servers": "kafkaserver:9092",
                        "topics": ["{INPUT_TOPIC}"],
                        "auto.offset.reset": "earliest"
                    }}
                }},
                "format": {{
                    "name": "json",
                    "config": {{
                        "update_format": "insert_delete",
                        "array": False
                    }}
                }}
            }}
        ]'
    );
    """)
    sql.sql(f"""
    CREATE VIEW {VIEW_NAME}
    WITH (
        'connectors' = '[
            {{
                "name": "kafka-3",
                "transport": {{
                    "name": "kafka_output",
                    "config": {{
                        "bootstrap.servers": "kafkaserver:9092",
                        "topic": "{OUTPUT_TOPIC}",
                        "auto.offset.reset": "earliest"
                    }}
                }},
                "format": {{
                    "name": "json",
                    "config": {{
                        "update_format": "insert_delete",
                        "array": False
                    }}
                }}
            }}
        ]'
    )
    AS SELECT COUNT(*) as num_rows FROM {TABLE_NAME};
    """)

    out = sql.listen(VIEW_NAME)
    sql.start()
    sql.wait_for_idle()
    sql.shutdown()
    df = out.to_pandas()

Ingesting data from a URL
**************************


Feldera can ingest data from a user-provided URL into a SQL table.
The file is fetched using HTTP with the GET method.

More on the HTTP GET connector at: https://www.feldera.com/docs/connectors/sources/http-get

.. note::
    The JSON used as input for Feldera should be in
    `newline-delimited JSON (NDJSON) format <https://www.feldera.com/docs/api/json/#encoding-multiple-changes>`_.


.. highlight:: python
.. code-block:: python

    sql = SQLContext("test_http_get", TEST_CLIENT).get_or_create()

    sql.sql("""
    CREATE TABLE items (
        id INT,
        name STRING
    ) WITH (
        'connectors' = '[
            {
                "name": "url_conn",
                "transport": {
                    "name": "url_input",
                    "config": {
                        "path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete",
                        "array": false
                    }
                }
            }
        ]'
    );

    CREATE VIEW s AS SELECT * FROM items;
    """)

    out = sql.listen("s")

    sql.start()
    sql.wait_for_completion(True)

    df = out.to_pandas()


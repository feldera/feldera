Examples
~~~~~~~~

Connecting to Feldera Sandbox
=============================

.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder

    client = FelderaClient('https://try.feldera.com', api_key=api_key)

    pipeline = PipelineBuilder(client, name, sql).create()

Connecting to Feldera on localhost
==================================

.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder

    client = FelderaClient('https://try.feldera.com', api_key=api_key)

    pipeline = PipelineBuilder(client, name, sql).create()

Creating a Pipeline
===================

.. code-block:: python

    sql = """
    CREATE TABLE student (
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

    pipeline = PipelineBuilder(client, name="notebook", sql=sql).create_or_replace()

Starting a Pipeline
===================

.. code-block:: python

    pipeline.start()


Using Pandas DataFrames
=======================

.. code-block:: python

    # populate pandas dataframes
    df_students = pd.read_csv('students.csv')
    df_grades = pd.read_csv('grades.csv')

    # subscribe to listen to outputs from a view
    out = pipeline.listen("average_scores")

    pipeline.start()

    # feed pandas dataframes as input
    pipeline.input_pandas(TBL_NAMES[0], df_students)
    pipeline.input_pandas(TBL_NAMES[1], df_grades)

    # wait for the pipeline to complete and shutdown
    pipeline.wait_for_completion(True)

    # get the output of the view as a pandas dataframe
    df = out.to_pandas()

    # delete the pipeline
    pipeline.delete()

Iterating over Output Chunks
============================

Use :meth:`.foreach_chunk` to process each chunk of data from a view or table.
It takes a callback, and calls the callback on each chunk of received data.

.. code-block:: python

    # define your callback to run on every chunk of data received
    # ensure that it takes two parameters, the chunk (DataFrame) and the sequence number
    def callback(df: pd.DataFrame, seq_no: int):
        print(f"\nSeq No: {seq_no}, DF size: {df.shape[0]}\n")

    pipeline = PipelineBuilder(client, name="notebook", sql=sql).create_or_replace()

    # register the callback for data received from the selected view
    pipeline.foreach_chunk(view_name, callback)

    # run the pipeline
    pipeline.start()
    pipeline.input_pandas(table_name, df)

    # wait for the pipeline to finish and shutdown
    pipeline.wait_for_completion(True)
    pipeline.delete()

Waiting for Completion
======================

To wait (block) till the pipeline has been completed, use :meth:`.Pipeline.wait_for_completion`.

.. code-block:: python

    pipeline.wait_for_completion()

Optionally, to shutdown the pipeline after completion:

.. code-block:: python

    pipeline.wait_for_completion(shutdown=True)

.. warning::
  If the data source is streaming, this will block forever.

End-to-End Example with Kafka Sink
==================================

This example shows creating and running a pipeline with Feldera's internal data generator and writing to a Kafka sink.

.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder

    client = FelderaClient('http://localhost:8080')

    sql = """
            CREATE TABLE Stocks (
            symbol VARCHAR NOT NULL,
            price_time BIGINT NOT NULL,  -- UNIX timestamp
            price DOUBLE NOT NULL
            ) with (
              'connectors' = '[{
                "transport": {
                  "name": "datagen",
                  "config": {
                    "plan": [{
                        "limit": 5,
                        "rate": 1,
                        "fields": {
                            "symbol": { "values": ["AAPL", "GOOGL", "SPY", "NVDA"] },
                            "price": { "strategy": "uniform", "range": [100, 10000] }
                        }
                    }]
                  }
                }
              }]'
            );

            CREATE VIEW googl_stocks
            WITH (
                'connectors' = '[
                    {
                        "name": "kafka-3",
                        "transport": {
                            "name": "kafka_output",
                            "config": {
                                "bootstrap.servers": "localhost:9092",
                                "topic": "googl_stocks",
                                "auto.offset.reset": "earliest"
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
            )
            AS SELECT * FROM Stocks WHERE symbol = 'GOOGL';
            """

    pipeline = PipelineBuilder(client, name="kafka_example", sql=sql).create_or_replace()

    out = pipeline.listen("googl_stocks")
    pipeline.start()

    # important: `wait_for_completion` will block forever here
    pipeline.wait_for_idle()
    pipeline.shutdown()
    df = out.to_pandas()
    assert df.shape[0] != 0

    pipeline.delete()

Specifying Data Sources / Sinks
===============================

To connect Feldera to various data sources or sinks, you can define them in the SQL code.
Refer to the connector documentation at: https://docs.feldera.com/connectors/

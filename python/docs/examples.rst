Examples
~~~~~~~~

Connecting to Feldera Sandbox
=============================

Ensure that you have an API key to connect to Feldera Sandbox.

To get the key:

- Login to the Feldera Sandbox.
- Click on the top right button that says: "Logged in"
- Click on "Manage API keys"
- Generate a new API key
- Give it a name, and copy the API key

.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder

    client = FelderaClient('https://try.feldera.com', api_key=api_key)

    pipeline = PipelineBuilder(client, name, sql).create()

Connecting to Feldera on localhost
==================================

.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder

    client = FelderaClient('http://127.0.0.1:8080', api_key=api_key)

    pipeline = PipelineBuilder(client, name, sql).create()

Setting HTTP Connection Timeouts
================================

To set a timeout for the HTTP connection, pass the timeout parameter to the `.class:FelderaClient` constructor.
If the Feldera backend server takes longer than the specified timeout to respond, a
`.class:FelderaTimeoutError` exception will be raised.
This example sets the timeout for each HTTP request to 10 seconds.

.. code-block:: python

    from feldera import FelderaClient, PipelineBuilder

    client = FelderaClient("http://127.0.0.1:8080", api_key=api_key, timeout=10)

.. note::
    This is for an individual HTTP request, and does not affect things like waiting for a pipeline to start,
    pause, resume and stop.
    To set a timeout for these state transitions, set the parameter `timeout_s` in respective functions.

Creating a Pipeline (OVERWRITING existing pipelines)
====================================================

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

    # This will stop and overwrite any existing pipeline with the same name.
    pipeline = PipelineBuilder(client, name="notebook", sql=sql).create_or_replace()

Creating a Pipeline with Fault Tolerance Enabled
================================================

.. code-block:: python

    from feldera.runtime_config import RuntimeConfig
    from feldera.enums import FaultToleranceModel

    client = FelderaClient.localhost()
    runtime_config = RuntimeConfig(
        fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
        checkpoint_interval_secs=60
    )

    pipeline = PipelineBuilder(client, name, sql, runtime_config=runtime_config).create()

Runtime configuration of a Pipeline
===================================

.. code-block:: python

    from feldera.runtime_config import RuntimeConfig

    client = FelderaClient.localhost()
    config = {
        "workers": 8,
        "storage": {
            "backend": {
                "name": "default"
            },
            "min_storage_bytes": None,
            "min_step_storage_bytes": None,
            "compression": "default",
            "cache_mib": None
        },
        "fault_tolerance": {
            "model": "at_least_once",
            "checkpoint_interval_secs": 60
        },
        "cpu_profiler": True,
        "tracing": False,
        "tracing_endpoint_jaeger": "",
        "min_batch_size_records": 0,
        "max_buffering_delay_usecs": 0,
        "resources": {
            "cpu_cores_min": None,
            "cpu_cores_max": None,
            "memory_mb_min": None,
            "memory_mb_max": None,
            "storage_mb_max": None,
            "storage_class": None
        },
        "clock_resolution_usecs": 1_000_000,
        "pin_cpus": [],
        "provisioning_timeout_secs": None,
        "max_parallel_connector_init": None,
        "init_containers": None,
        "checkpoint_during_suspend": True,
        "dev_tweaks": {}
    }

    runtime_config = RuntimeConfig.from_dict(config)

    pipeline = PipelineBuilder(client, name, sql, runtime_config=runtime_config).create()


Starting a Pipeline
===================

.. code-block:: python

    pipeline.start()

Analyzing Existing Feldera Pipeline for Errors
==============================================

First let's create a Feldera pipeline that errors from the web console, with the
name ``check_error`` and invalid SQL as follows:

.. code-block:: sql

   SELECT invalid

This will fail to compile.

We can use this Python SDK to connect to this Feldera pipeline to check if it has
any errors as follows:

.. code-block:: python

    pipeline = Pipeline.get("check_error", client)
    err = pipeline.errors()

    if len(err) != 0:
        print("got err: ", err)

Here, ``err`` is a list of all errors in this pipeline. The above code will emit
the following output:

.. code-block:: text

   got err:  [{'sql_compilation': {'exit_code': 1, 'messages': [{'start_line_number': 1, 'start_column': 1, 'end_line_number': 1, 'end_column': 14, 'warning': False, 'error_type': 'Not supported', 'message': 'Raw \'SELECT\' statements are not supported; did you forget to CREATE VIEW?: SELECT "invalid"', 'snippet': '    1|SELECT invalid\n      ^^^^^^^^^^^^^^\n'}]}}]


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
    pipeline.input_pandas("students", df_students)
    pipeline.input_pandas("grades", df_grades)

    # wait for the pipeline to complete and stop
    pipeline.wait_for_completion(True)

    # get the output of the view as a pandas dataframe
    df = out.to_pandas()

    # clear the storage and delete the pipeline
    pipeline.delete(True)

Executing ad-hoc SQL Queries
============================

Ad-hoc SQL queries can be executed on running or paused pipelines.
Ad-hoc queries provide a way to query the state of **materialized** views or tables.

For more information, refer to the docs at: https://docs.feldera.com/sql/ad-hoc

We provide the following methods to execute ad-hoc queries:

#. :meth:`.Pipeline.execute` - Execute an ad-hoc query and discard the result. Useful for ``INSERT`` queries.

#. :meth:`.Pipeline.query` **(Lazy)** - Executes an ad-hoc query and returns a generator to iterate over the result.

#. :meth:`.Pipeline.query_tabular` **(Lazy)** - Executes an ad-hoc query and returns a generator that yields a string representing the query result in human-readable tabular format.

#. :meth:`.Pipeline.query_parquet` - Executes an ad-hoc query and saves the result to the specified path as a parquet file.

.. code-block:: python

    # execute an `INSERT` ad-hoc SQL query
    pipeline.execute("INSERT into students VALUES ('John', 1)")

    # executing a `SELECT` ad-hoc SQL query
    students = list(pipeline.query("SELECT * FROM students"))

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
    pipeline.foreach_chunk("view_name", callback)

    # run the pipeline
    pipeline.start()
    pipeline.input_pandas("table_name", df)

    # wait for the pipeline to finish and stop
    pipeline.wait_for_completion(True)

    # clear the storage and delete the pipeline
    pipeline.delete(True)

Waiting for Completion
======================

To wait (block) till the pipeline has been completed, use :meth:`.Pipeline.wait_for_completion`.

.. code-block:: python

    pipeline.wait_for_completion()

Optionally, to forcibly stop (without checkpointing) the pipeline after completion:

.. code-block:: python

    pipeline.wait_for_completion(force_stop=True)

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
            price DECIMAL(38, 2) NOT NULL
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
    pipeline.stop(force=True)
    df = out.to_pandas()
    assert df.shape[0] != 0

    # clear the storage and delete the pipeline
    pipeline.delete(True)

Retrieve a support-bundle for a pipeline
==================================

This example shows how to download a support bundle for a pipeline using the Python SDK.

.. code-block:: python

    # Create a client (assuming Feldera is running on localhost:8080)
    client = FelderaClient.localhost(port=8080)

    # Define a simple SQL program
    sql_program = """
    CREATE TABLE users(id INT, name STRING);
    CREATE MATERIALIZED VIEW user_count AS SELECT COUNT(*) as count FROM users;
    """

    # Create a pipeline
    pipeline_name = "support-bundle-example"
    pipeline = PipelineBuilder(
        client,
        pipeline_name,
        sql_program
    ).create_or_replace()

    print(f"Created pipeline: {pipeline.name}")

    # Start the pipeline
    pipeline.start()
    print("Pipeline started")

    # Generate support bundle as bytes
    print("Generating support bundle...")
    support_bundle_bytes = pipeline.support_bundle()
    print(f"Support bundle size: {len(support_bundle_bytes)} bytes")

    # Verify it's a valid ZIP file
    try:
        with zipfile.ZipFile(io.BytesIO(support_bundle_bytes), 'r') as zip_file:
            file_list = zip_file.namelist()
            print(f"Support bundle contains {len(file_list)} files:")
            for file_name in file_list[:5]:  # Show first 5 files
                print(f"  - {file_name}")
            if len(file_list) > 5:
                print(f"  ... and {len(file_list) - 5} more files")
    except zipfile.BadZipFile:
        print("Warning: Support bundle is not a valid ZIP file")

    # Save support bundle to a file
    output_path = f"{pipeline_name}-support-bundle.zip"
    pipeline.support_bundle(output_path=output_path)
    print(f"Support bundle saved to: {output_path}")

    # Verify the saved file
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        print(f"Saved file size: {file_size} bytes")

        # Clean up
        os.unlink(output_path)
        print("Cleaned up saved file")

    # Stop the pipeline
    pipeline.stop(force=True)
    pipeline.clear_storage()
    pipeline.delete()
    print("Pipeline stopped and deleted")

Specifying Data Sources / Sinks
===============================

To connect Feldera to various data sources or sinks, you can define them in the SQL code.
Refer to the connector documentation at: https://docs.feldera.com/connectors/

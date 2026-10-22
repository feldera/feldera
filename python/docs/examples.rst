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

TLS with Self Signed Certificates
=================================

To use TLS with self signed certificates, set to the path of the CA bundle or
the directory that contains the CA certificates.

.. code-block:: python

   from feldera import FelderaClient

   client = FelderaClient('https://cluster.feldera.com', verify='path/to/cert')

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
        checkpoint_interval="60s"
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
            "min_spill_size": "10Mi",
            "compression": "default",
            "cache_size": "4Gi"
        },
        "fault_tolerance": {
            "model": "at_least_once",
            "checkpoint_interval": "60s"
        },
        "cpu_profiler": True,
        "tracing": False,
        "tracing_endpoint_jaeger": "",
        "min_batch_size_records": 0,
        "max_buffering_delay": "0us",
        "resources": {
            "cpu_cores_min": None,
            "cpu_cores_max": None,
            "memory_min": None,
            "memory_max": "16Gi",
            "storage_max": "400Gi",
            "storage_class": None
        },
        "clock_resolution": "1s",
        "pin_cpus": [],
        "provisioning_timeout": None,
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

Approving Reloads of Selected Tables when Bootstrapping
=======================================================

When a modified pipeline restarts from a checkpoint, if a table is modified,
bootstrapping will reload the table: it truncates the table and resets its input
connectors. With ``bootstrap_policy=BootstrapPolicy.ALLOW``, and ``wait=True``,
``reloadable_tables``  lists the tables that bootstrapping may reload.
If a modified table is not in the list, ``start()`` raises ``TableReloadNotAllowedError``
and the pipeline stays in the ``AWAITINGAPPROVAL`` state.
Bootstrapping is an Enterprise feature.

.. code-block:: python

    from feldera.enums import BootstrapPolicy
    from feldera.rest.errors import TableReloadNotAllowedError

    try:
        pipeline.start(
            bootstrap_policy=BootstrapPolicy.ALLOW,
            reloadable_tables=["dim_customers", "dim_products"],
        )
    except TableReloadNotAllowedError as e:
        print(f"Bootstrapping would reload: {e.blocked_tables}")
        print(e.approval_diff)

        # Reject: stop the pipeline. A force stop makes no checkpoint.
        pipeline.stop(force=True)

        # Or approve: bootstrap with the changes as they are.
        # pipeline.approve()

Write each name exactly as the pipeline reports it in the approval diff: an
unquoted SQL name in lowercase, a quoted name without its quotes. For example,
write ``dim_customers`` for ``CREATE TABLE DIM_CUSTOMERS`` and ``DimCustomers``
for ``CREATE TABLE "DimCustomers"``. The names in ``e.blocked_tables`` use the
same spelling, so you can pass them back:

.. code-block:: python

    pipeline.restart(
        bootstrap_policy=BootstrapPolicy.ALLOW,
        reloadable_tables=["dim_customers", "dim_products", *e.blocked_tables],
    )

Use ``reloadable_tables=[]`` to allow no table reloads. Changes to views only
are still approved.

For more information, refer to the docs at:
https://docs.feldera.com/pipelines/modifying#approving-reloads-of-selected-tables-python-sdk

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

    pipeline.start()

    # subscribe to listen to outputs from a view
    out = pipeline.listen("average_scores")

    # feed pandas dataframes as input
    pipeline.input_pandas("students", df_students)
    pipeline.input_pandas("grades", df_grades)

    # wait for the pipeline to complete and stop
    pipeline.wait_for_completion(True)

    # get the output of the view as a pandas dataframe
    df = out.to_pandas()

    # clear the storage and delete the pipeline
    pipeline.delete(True)

Using Completion Tokens
=======================

`Completion tokens <https://docs.feldera.com/connectors/completion-tokens/#completion-tokens>`_ can be used to check whether
all inputs ingested before the token was issued have been fully processed, and the resulting outputs have been written to all data sinks.

The following methods automatically use completion tokens:

- :meth:`.Pipeline.input_json`
- :meth:`.Pipeline.input_pandas`

To generate a completion token for a connector attached to the pipeline use :meth:`.Pipeline.generate_completion_token`.
To check the status of this completion token use :meth:`.Pipeline.completion_token_status`.

.. code-block:: python

    # generate completion token for table "t0" and connector "myconnector"
    token = pipeline.generate_completion_token("t0", "myconnector")

    # check the status of this completion token
    status = pipeline.completion_token_status(token)
    print(status)

    # wait until the pipeline processes this completion token
    pipeline.wait_for_token(token)

:meth:`.Pipeline.wait_for_token` returns once the pipeline has handed the
resulting changes to its output connectors.  A listener from
:meth:`.Pipeline.listen` receives its changes over an HTTP stream, so they can
still be on their way.  :meth:`.OutputHandler.wait_for_token` returns once the
listener has received every change for the token, and once the pipeline has
confirmed that the token changed nothing in the view:

.. code-block:: python

    totals = pipeline.listen("customer_totals")
    token = pipeline.input_json("orders", [{"customer": "alice", "amount": 10}])

    # every change that the input caused in "customer_totals" is in the listener
    totals.wait_for_token(token)
    print(totals.to_dict())

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

    # run the pipeline
    pipeline.start()

    # register the callback for data received from the selected view
    pipeline.foreach_chunk("view_name", callback)

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

    # Start the pipeline in paused state, attach listener, then unpause the pipeline.
    # This ensures that the listener gets all the output from the view.
    pipeline.start_paused()
    out = pipeline.listen("googl_stocks")
    pipeline.resume()

    # important: `wait_for_completion` will block forever here
    pipeline.wait_for_idle()
    pipeline.stop(force=True)
    df = out.to_pandas()
    assert df.shape[0] != 0

    # clear the storage and delete the pipeline
    pipeline.delete(True)

Retrieve a support-bundle for a pipeline
========================================

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

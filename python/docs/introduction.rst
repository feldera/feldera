Introduction
============

The Feldera Python SDK is meant to provide an easy and convenient way of
interacting with Feldera.


Please submit any feature request / bug reports to:
https://github.com/feldera/feldera


Installation
*************

.. code-block:: bash

   pip install feldera


Installing from Github:

.. code-block:: bash

   pip install git+https://github.com/feldera/feldera#subdirectory=python


Similarly, to install from a specific branch:

.. code-block:: bash

   $ pip install git+https://github.com/feldera/feldera@{BRANCH_NAME}#subdirectory=python

Replace ``{BRANCH_NAME}`` with the name of the branch you want to install from.

Installing from Local Directory:

If you have cloned the Feldera repo, you can install the python SDK as follows:

.. code-block:: bash

   # the Feldera Python SDK is present inside the python/ directory
   pip install python/

Key Concepts
************

* :class:`.FelderaClient`
   - This is the actual HTTP client used to make requests to your Feldera
     instance.
   - creating an instance of :class:`.FelderaClient` is usually the first thing you
     will do while working with Feldera.

   - Example:

      .. code-block:: python

         from feldera import FelderaClient

         client = FelderaClient("https://try.feldera.com", api_key="YOUR_API_KEY")

      - The API key may not be required if you are running Feldera locally.



* :class:`.PipelineBuilder`
    Builder class for constructing new Feldera pipelines. Configure the builder with pipeline name, SQL code, and other optional attributes and call one of two methods to create the pipeline, possibly overwriting an existing pipeline with the same name:

        - :meth:`.PipelineBuilder.create`
        - :meth:`.PipelineBuilder.create_or_replace`
        - :meth:`.PipelineBuilder.get`

    - Example:

      .. code-block:: python

         from feldera import PipelineBuilder

         pipeline = PipelineBuilder(client, name="example", sql=sql).create()

* :meth:`.Pipeline.start`
   - Starts the Feldera Pipeline and keeps it running indefinitely.
   - Example:

      .. code-block:: python

         pipeline.start()

      - This tells Feldera to go ahead and start processing the data.

* :meth:`.Pipeline.wait_for_completion`
   - Blocks this pipeline until completion, i.e., until the end-of-file (EOI)
     has been reached for all input sources.

   - Takes a parameter ``force_stop``, when set stops the pipeline down after completion.

   - Example:

      .. code-block:: python

         from feldera import FelderaClient, PipelineBuilder
         import pandas as pd

         tbl_name = "user_data"
         view_name = "select_view"

         sql = f"""
            -- Declare input tables
            CREATE TABLE {tbl_name} (name STRING);
            -- Create Views based on your queries
            CREATE VIEW {view_name} AS SELECT * FROM {tbl_name};
         """

         client = FelderaClient("https://try.feldera.com", api_key="YOUR_API_KEY")
         pipeline = PipelineBuilder(client, name="example", sql=sql).create()

         # start the pipeline
         pipeline.start()

         # read input data
         df = pd.read_csv("data.csv")
         pipeline.input_pandas(tbl_name, df)

         # wait for the pipeline to complete
         pipeline.wait_for_completion(force_stop=True)

      - Write the SQL query that creates a table and a view.
        You can also create other views on top of existing views.
      - Create a :class:`.PipelineBuilder` and build the pipeline.
      - Call :meth:`.Pipeline.start` to start the pipeline.
      - Pass a pandas DataFrame as input to the table.
      - Finally, wait for the the pipeline to complete.

   .. warning::
      If the data source is streaming, this will block forever.
      In such cases, use :meth:`.Pipeline.wait_for_idle` instead.

Checkout the :doc:`/examples`.

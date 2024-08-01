Introduction
============

The Feldera Python SDK is meant to provide an easy and convenient way of
interacting with Feldera.


Please submit any feature request / bug reports to:
https://github.com/feldera/feldera


Installation
*************

.. code-block:: bash

   pip install git+https://github.com/feldera/feldera#subdirectory=python


Similarly, to install from a specific branch:

.. code-block:: bash

   $ pip install git+https://github.com/feldera/feldera@{BRANCH_NAME}#subdirectory=python

Replace ``{BRANCH_NAME}`` with the name of the branch you want to install from.


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



* :class:`.SQLContext`
   - This represents the current context of your SQL program, data sources
     and sinks. In Feldera terminology, this represents both a Program and a
     Pipeline.

   - Example:

      .. code-block:: python

         from feldera import SQLContext

         sql = SQLContext("getting_started", client).get_or_create()

      - The first parameter is the name of this SQL context. By default, this is
        the name used in both Feldera Program and Pipeline.
      - The second parameter here is :class:`.FelderaClient` that we created above.

* :meth:`.SQLContext.wait_for_completion`
   - Blocks this Feldera pipeline until completion. Normally this means until the end-of-file (EOF)
     has been reached for this input source.

   - Takes a parameter ``shutdown``, when set shuts the pipeline down after completion.

   - Example:

      .. code-block:: python

         from feldera import SQLSchema
         import pandas as pd

         tbl_name = "user_data"
         view_name = "select_view"

         # read input data
         df = pd.read_csv("data.csv")

         # Declare input tables
         sql.sql(f"CREATE TABLE {tbl_name} (name STRING);")
         # Create Views based on your queries
         sql.sql(f"CREATE VIEW {view_name} AS SELECT * FROM {tbl_name};")

         sql.start()
         sql.input_pandas(df)
         sql.wait_for_completion(shutdown=True)

      - Here, we create a table which can receive data from input sources.
      - Then, we create a view that performs operations on this input data.
        You can also create other views on top of existing views.
      - We start the Feldera pipeline we have just created.
      - Then, we pass a pandas DataFrame as input to the table.
      - Finally, we wait for the the pipeline to complete.

   .. warning::
      If the data source is streaming, this will block forever.
      In such cases, use :meth:`.SQLContext.start` instead.

* :meth:`.SQLContext.start`
   - Starts the Feldera Pipeline and keeps it running indefinitely.
   - Example:

      .. code-block:: python

         sql.start()

      - This tells Feldera to go ahead and start processing the data.

Checkout the :doc:`/examples`.

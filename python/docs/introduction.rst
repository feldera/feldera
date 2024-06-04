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

* :meth:`.SQLContext.run_to_completion`
   - Runs this Feldera pipeline to completion. Normally this means until the EoF
     has been reached for this input source.

   - Example:

      .. code-block:: python
         
         from feldera import SQLSchema

         tbl_name = "user_data"
         view_name = "select_view"

         # Declare input tables
         sql.register_table(tbl_name, SQLSchema({"name": "STRING"}))

         # Register Views based on your queries
         query = f"SELECT * FROM {tbl_name}"
         sql.register_view(view_name, query)

         # name for this connector
         in_con = "delta_input_conn"

         # the configuration for this input source
         in_cfg = {...}

         sql.connect_source_delta_table(tbl_name, in_con, in_cfg)

         # name for this connector
         out_con = "delta_output_con"

         # the configuration for this input source
         out_cfg = {...}

         sql.connect_sink_delta_table(view_name, out_con, out_cfg)

         sql.run_to_completion()

      - Here, we register a data table which receives data from input sources.
      - Then, we register a view that performs operations on this input data. 
        You can also register other views on top of existing views.
      - Then, we connect a source delta table to the previously defined table.
      - Then, we connect a sink delta table to the previously defined view.
      - Finally, we run the pipeline to completion. Feldera will fetch data from
        the source, perform the query you supplied and passes this data to the 
        sink delta table.

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

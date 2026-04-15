{#
    Kafka-sourced sales table.

    Receives denormalized sales events from the 'sales_events' Kafka topic.
    Each record contains the IDs needed to join with dimension tables and
    the order line-item details. Records are UNION'd into fct_sales so that
    Kafka data flows through obt_sales → Delta Lake automatically.

    Uses start_from=latest so no historical messages are consumed on a fresh
    deploy — only data produced after the pipeline starts is processed.
#}
salesorderid INTEGER NOT NULL,
salesorderdetailid INTEGER NOT NULL,
productid INTEGER NOT NULL,
customerid INTEGER NOT NULL,
creditcardid INTEGER,
shiptoaddressid INTEGER NOT NULL,
order_status INTEGER NOT NULL,
orderdate VARCHAR NOT NULL,
orderqty INTEGER NOT NULL,
unitprice DOUBLE NOT NULL

# Part 1: Writing and testing your first SQL pipeline

In this section of the tutorial we will write and test our first SQL pipeline using Feldera.

## The use case

We will build a pipeline that ingests data about
vendors, parts, and prices, and continuously tracks the lowest available
price for each part across all vendors.

## Step 0. Launch Feldera

Make sure that you have Feldera up and running by following the [Getting
Started](/docker.md) guide.  Open the Feldera Web Console on
[localhost:8080](http://localhost:8080).

## Step 1. Create a pipeline

In the Feldera Web Console,
create a new pipeline, called named "supply_chain", and paste the following code
in the SQL editor:

```sql
create table VENDOR (
    id bigint not null primary key,
    name varchar,
    address varchar
) with ('materialized' = 'true');

create table PART (
    id bigint not null primary key,
    name varchar
) with ('materialized' = 'true');

create table PRICE (
    part bigint not null,
    vendor bigint not null,
    price decimal
) with ('materialized' = 'true');

-- Lowest available price for each part across all vendors.
create view LOW_PRICE (
    part,
    price
) as
    select part, MIN(price) as price from PRICE group by part;

-- Lowest available price for each part along with part and vendor details.
create materialized view PREFERRED_VENDOR (
    part_id,
    part_name,
    vendor_id,
    vendor_name,
    price
) as
    select
        PART.id as part_id,
        PART.name as part_name,
        VENDOR.id as vendor_id,
        VENDOR.name as vendor_name,
        PRICE.price
    from
        PRICE,
        PART,
        VENDOR,
        LOW_PRICE
    where
        PRICE.price = LOW_PRICE.price AND
        PRICE.part = LOW_PRICE.part AND
        PART.id = PRICE.part AND
        VENDOR.id = PRICE.vendor;
```

The first part of this listing declares inputs to the pipeline
using SQL `CREATE TABLE` statements.
Indeed, SQL's data modeling language works for streaming
data just as well as for tables stored on the disk.  No need to learn a new
language: if you know SQL, you already know streaming SQL!

Note that these declarations do not say anything
about the sources of data.  We will add that in Part 3 of the tutorial.

Finally, note the `'materialized' = 'true'` attribute on the
tables.  This annotation instructs Feldera to store the entire contents of the table,
so that the user can browse it at any time.

The second part of the listing defines queries on top of the input tables.
In Feldera we write queries as SQL views.
Views can be defined in terms of
tables and other views, making it possible to express deeply nested queries
in a modular way.
In this example we compute the lowest price for each part
across all vendors as the `LOW_PRICE` view. We then define the `PREFERRED_VENDOR`
view on top of `LOW_PRICE`.

We declare `PREFERRED_VENDOR` as a **materialized** view, instructing Feldera to
store the entire contents of the view, so that the user can browse it at any time.
This is in contrast to regular views, for which the user can only observe a stream
of **changes** to the view, but cannot inspect its current contents.

Click the PLAY button to run the pipeline.

## Step 2. Populate tables manually

:::info UNDER CONSTRUCTION

This section is under construction.

:::

<!-- ## Step 3. Make changes

:::note

The Web Console does not yet support deleting records.  Use the REST API
described in the next part of the tutorial instead.

:::

## Step 6. Stop the pipeline

Click the stop icon <icon icon="bx:stop-circle" /> to shut down the pipeline.

:::caution

All pipeline state will be lost.

::: -->

<!-- ## Takeaways

Let us recap what we have learned so far:

- Feldera executes **programs** written in standard SQL, using `CREATE TABLE` and `CREATE VIEW` statements.
  - `CREATE TABLE` statements define a schema for input data.
  - `CREATE VIEW` statements define queries over input tables and other views.

- A SQL program is instantiated as part of a **pipeline**.

- Feldera evaluates queries **continuously**, updating their results
  as input data changes. -->

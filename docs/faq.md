# Feldera Frequently Asked Questions

## What is Feldera

Feldera is an early-stage start-up building a new data processing
engine for *streaming data*.

Using industry jargon, the Feldera engine can be described in several ways:

- a streaming analytics query engine
- a continuous query engine
- a standing query engine
- an incremental view maintenance engine
- implementing materialized views

## What do you mean by "streaming data"?

Data that continuously changes, usually at a high rate.  A change of
the data can encompass an arbitrary combination of insertions,
deletions, or updates in one or multiple data sources.  Feldera is
designed for database-like data: numbers, strings, dates, JSON.

## How about streaming video?

Feldera is not designed for media processing.

## What are the "killer apps" for Feldera?

There are several types of problems for which Feldera may be the *only*
viable solution:

- when data arrives *too fast* to process using other tools.  For
  example, other tools take 5 minutes to perform a result needed every
  minute.

- when the analysis needs to combine data from *multiple sources* or
  when the analysis is *complex*: other tools are frequently limited
  in these dimensions

- when data analysis is *too expensive*.  Sometimes you *can* use a
  very expensive parallel engine to analyze large data volumes, but
  the analysis would be much cheaper using Feldera, since it is using
  many fewer resources.

## How is the Feldera engine programmed?

Feldera is programmed in SQL.  Feldera supports essentially the entire
SQL language.  There exists a low-level Rust API as well, but Feldera's
tooling is geared towards the SQL API.

## What does Feldera do that a streaming engine cannot do?

Feldera is the only engine than can evaluate arbitrary SQL over streaming data, while supporting insertions, updates and deletes, from multiple sources. Other streaming engines typically support restrictive subsets of SQL, and often only support insertions.

## What does Feldera do that a database cannot do?

Database engines cannot efficiently maintain multiple complex views on
fast-changing data.

## How can you write streaming programs in SQL?

The SQL programs use SQL *tables* to describe data sources, and SQL
*views* to describe the computed results.  Then, as the input tables
are modified, Feldera updates the views so that they are always
up-to-date.

## How fast is Feldera?

This depends on a lot of factors, but for typical queries Feldera can
compute the results in a matter of milliseconds after each change, and
process millions of records per second.

## Why is Feldera fast?

Feldera uses a technique called "Incremental View Maintenance".  The
idea is to compute only as much as necessary based on the *changes* of
the inputs.  Often the changes are much smaller than the whole input.
This means that computing based on the changes can be millions of
times faster than computing the result from scratch.

## Don't databases offer incremental view maintenance?

Databases can only evaluate some restricted types of views
incrementally.  Feldera can do this for essentially any program you
can write in SQL.

## Is Feldera a database?

No.  Feldera only offers the data processing engine, but does not
store the data persistently for later retrieval.  Moreover, in Feldera
queries need to be written *before* the data starts arriving.

## Is Feldera a stateless system (lambda)?

No.  Feldera computations do maintain internal state that may be
needed for computing future results.

## Where does Feldera run?

Feldera is designed to run as a cloud service.  It can be deployed as
a service in:

- Managed public clouds
- Virtual public clouds
- On premise
- As a single-node docker-based image for developers

## Where is the data coming from?

A Feldera program can ingest data from any number and variety of data
sources.

Data can be sent to Feldera using http requests, or Feldera can use
connectors to retrieve data from a variety of sources (e.g., Kafka,
databases, data lakes, etc.).  If there is no connector for your
favorite data source we can usually prototype one in a few days.

## Where is the output?

Feldera is designed to work as a service which notifies users of changes to
all the views they defined, whenever these changes happen.

Feldera can write outputs to a variety of destinations (e.g.,
databases, Kafka queues, data lakes).

## I want the views, not the changes.

Feldera can do that too, but this may be more expensive when the views
are much larger than their changes.

## Is Feldera open-source?

The Feldera engine and compiler are licensed under the MIT license.
The code is available at <https://github.com/feldera/feldera>

## Does Feldera have fault-tolerance?

The code making Feldera fault-tolerant is almost complete.  We expect that
it will be ready within the next two months.

## Can Feldera scale-out (use many computers)?

Feldera can use many processors of a single server.  The code for
scaling Feldera to use multiple computers is under development.  We
expect that it will be ready within the next 6 months.

## How can I try Feldera?

- You can try our online sandbox: <https://try.feldera.com>

- You can download a docker image that offers everything packaged:
  <https://www.feldera.com/docs/docker#start-the-demo>

- We will be very happy to help design a prototype solution for your
  particular problem.

## There are literally a hundred companies and start-ups doing this

Indeed.  Our secret weapon is inventing a new theory of streaming data
processing, which received the [best paper
award](https://vldb.org/2023/?conference-awards) at the 2023
conference on Very Large Databases.

Other solutions compromise either in the breadth of the operations
supported, or in the speed of processing.  Feldera is a streaming
query engine that makes no compromises.

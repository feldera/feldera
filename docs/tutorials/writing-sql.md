# Your first SQL program

Feldera only supports two kinds of SQL Data Definition Language (DDL)
statements: table definitions, and view definitions. Each table definition
becomes an input, and each view definition becomes an output. Here is an example
program:

```sql
CREATE TABLE Person
(
    name    VARCHAR,
    age     INT,
    present BOOLEAN
);
CREATE MATERIALIZED VIEW Adult AS SELECT Person.name, Person.age FROM Person WHERE Person.age > 18;
```

Statements need to be separated by semicolons.

## Incremental view maintenance

Feldera is optimized for performing incremental view
maintenance. In consequence, Feldera programs in SQL are expressed as
VIEWS, or *standing queries*.  A view is a virtual table that is
formed by a query on other tables or views.

For example, the following query defines a view:

```sql
CREATE MATERIALIZED VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18
```

In order to interpret this query the compiler needs to have been given
a definition of table (or view) Person.  The table `Person` must be
defined using a SQL DDL statement, e.g.:

```SQL
CREATE TABLE Person
(
    name    VARCHAR,
    age     INT,
    present BOOLEAN
)
```


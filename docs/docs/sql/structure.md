# High-level program structure

DBSP only supports two kinds of SQL statements: table definition
statements, and view definition statements.  Each table definition
becomes an input, and each view definition becomes an output.
Here is an example program:

```sql
CREATE TABLE T(COL0 INTEGER, COL1 INTEGER);
CREATE VIEW V AS SELECT T.COL1 FROM T;
```

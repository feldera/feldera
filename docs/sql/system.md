# System views

The Feldera runtime comes with some built-in views.  These views are
built-in.  Their contents is pre-populated by the runtime.

Currently they cannot be used as inputs to queries.

## `ERROR_VIEW`

`ERROR_VIEW` is a view that signals runtime errors.  The schema is
given by the following (illegal) SQL declaration:

```sql
CREATE VIEW ERROR_VIEW(
   table_or_view_name VARCHAR NOT NULL,
   message VARCHAR NOT NULL,
   metadata VARIANT NOT NULL
);
```

The first column contains a table or view name, whose computation is
responsible for the error.

The second column contains an error string.

The third column contains additional metadata describing the error.

Currently this view contains all late records that are filered away by
[`LATENESS` annotations](streaming.md).


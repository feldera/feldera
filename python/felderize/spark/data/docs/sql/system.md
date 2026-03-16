# System Views

The Feldera runtime comes with some built-in views.  These views are
built-in.  Their contents is pre-populated by the runtime.

## `ERROR_VIEW`

::: Warning

The `ERROR_VIEW` is still experimental; the schema and name of this
view may change.

:::

`ERROR_VIEW` is a view that signals runtime errors.  The schema is
given by the following (illegal) SQL declaration:

```sql
CREATE VIEW ERROR_VIEW(
   table_or_view_name VARCHAR NOT NULL,
   message VARCHAR NOT NULL,
   metadata VARCHAR NOT NULL
);
```

The first column contains a table or view name, whose computation is
responsible for the error.

The second column contains an error string.

The third column contains additional metadata describing the error.

Currently this view contains all late records that are filered away by
[`LATENESS` annotations](streaming.md).

(Currently the error view will *not* contain errors that are produced
from processing records that originate in the error view itself.)
# Operations on Maps

A MAP type can be created using the syntax MAP<type, type>.
For example `MAP<VARCHAR, INT>` is an map from strings to integers.

In `CREATE TABLE` and `CREATE TYPE` declarations there is no way to
specify the nullability of the values of a `MAP`.  The compiler will
always assume that map keys are *not* nullable, while values elements
are nullable:

```sql
CREATE TABLE T(m MAP<VARCHAR, INT>);
```

Table `T` will have a single column `m` whose values are nullable
maps, the map's keys are non-nullable `VARCHAR` values, while the
maps's values are nullable `INT` values.

## Map literals

Map literals have the syntax `MAP[`key `,` value ( `,` key `,` value )* `]`.
Here is an example: `SELECT MAP['hi',2]`.

## Predefined functions on map values

| Function                                                             | Description                                                                                                                                                                                                                                                                               | Example                                                             |
|----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| _map_`[`_key_`]`                                                 | Returns the element in the map with the specified key. If there is no such key, the result is `NULL`.                                                                 | `MAP['x', 4, 'y', 3]['x']` => 4                                                |

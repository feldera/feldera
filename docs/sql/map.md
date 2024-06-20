# Operations on Maps

A MAP type can be created using the syntax MAP<type, type>.
For example `MAP<VARCHAR, INT>` is an map from strings to integers.

## Map literals

Map literals have the syntax `MAP[`key `,` value ( `,` key `,` value )* `]`.
Here is an example: `SELECT MAP['hi',2]`.

## Predefined functions on map values

| Function                                                             | Description                                                                                                                                                                                                                                                                               | Example                                                             |
|----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| _map_`[`_key_`]`                                                 | Returns the element in the map with the specified key. If there is no such key, the result is `NULL`.                                                                 | `MAP['x', 4, 'y', 3]['x']` => 4                                                |

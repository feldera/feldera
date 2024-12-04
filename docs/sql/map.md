# Operations on Maps

A MAP type can be created using the syntax `MAP<type, type>`.
For example `MAP<VARCHAR, INT>` is a map from strings to integers.

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

## Comparison Operations on Maps

Comparison operations (`=`, `<>`, `!=`, `>`, `<`, `>=`, `<=`) can be applied to maps. First, the keys are sorted in lexicographical order. The comparison occurs on the keys first. If the keys are equal, their corresponding values are compared next.

Given a table `T` with columns `map1` and `map2` where `map1 = {"v": 11, "q": 66}` and `map2 = {"v": 22}`:
  - `SELECT map1 FROM T WHERE map1 < map2;` returns:
    - `'map1': {'q': 66, 'v': 11}`


## Predefined functions on map values

| Function               | Description                                                                                           | Example                                                              |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
| _map_`[`_key_`]`       | Returns the element in the map with the specified key. If there is no such key, the result is `NULL`.  | `MAP['x', 4, 'y', 3]['x']` => 4 |
| `CARDINALITY`(map)     | Returns the number of key-value pairs in the map.                                                      | `CARDINALITY(MAP['x', 4])` => 1 |
| `MAP_CONTAINS_KEY`(map, key) | Returns true when the map has an item with the specified key; `NULL` if any argument is `NULL`.  | `MAP_CONTAINS_KEY(MAP['x', 4], 'x')` => `true` |


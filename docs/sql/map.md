# Map Operations

A MAP type can be created using the syntax `MAP<type, type>`.
For example `MAP<VARCHAR, INT>` is a map from strings to integers.

In `CREATE TABLE` and `CREATE TYPE` declarations there is no way to
specify the nullability of the values of a `MAP`.  The compiler will
always assume that map keys are *not* nullable, while values elements
*are* nullable:

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

Comparison operations (`=`, `<>`, `!=`, `>`, `<`, `>=`, `<=`) can be applied to maps and work as follows:

1. The keys are first sorted **lexicographically**.
2. If the keys are not equal, the result is determined by their lexicographical order.
3. If the keys are equal, their corresponding values are compared next.
4. If the values are also equal, the comparison moves to the next key-value pair until a difference is found.

#### Examples:

1. Given a table `T` with columns `c1 = MAP['v', 11, 'q', 66]` and `c2 = MAP['v': 22]`:
   - `SELECT c1 FROM T WHERE c1 < c2;` returns:
     -  `MAP['q', 66, 'v', 11]`

2. Given two maps where previously compared key-value pairs are equal, such as:
   - `MAP['f', 1, 'v', 0]` and `MAP['f': 1]`
   - The map with more elements, `MAP['f', 1, 'v', 0]`, is considered larger.


## Predefined functions on map values

| Function               | Description                                                                                           | Example                                                              |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
| _map_`[`_key_`]`       | Returns the element in the map with the specified key. If there is no such key, the result is `NULL`.  | `MAP['x', 4, 'y', 3]['x']` => 4 |
| <a id="cardinality"></a>`CARDINALITY`(map)     | Returns the number of key-value pairs in the map.                                                      | `CARDINALITY(MAP['x', 4])` => 1 |
| <a id="map_contains_key"></a>`MAP_CONTAINS_KEY`(map, key) | Returns true when the map has an item with the specified key; `NULL` if any argument is `NULL`.  | `MAP_CONTAINS_KEY(MAP['x', 4], 'x')` => `true` |


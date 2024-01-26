# Operations on arrays

An array type can be created by applying the `ARRAY` suffix to
another type.  For example `INT ARRAY` is an array of integers.
Array indexes start from 1.  Array sizes are limited to 2^31 elements.
Array elements may be nullable types, e.g., `INT ARRAY NULL`.
Multidimensional arrays are possible, e.g. `VARCHAR ARRAY ARRAY`
is a two-dimensional array.

## Array literals

Array literals have the syntax `ARRAY[`expr [`,`expr]*`]`.  An example
creating a nested array is: `ARRAY[ARRAY[1, 2], ARRAY[3, 4]]`.

## The `UNNEST` SQL Operator

The `UNNEST` operator takes an `ARRAY` and returns a table with a
row for each element in the `ARRAY`: `UNNEST(ARRAY) [WITH
ORDINALITY]`.  If the input is an array with 5 elements, the output
is a table with 5 rows, each row holding one element of the array.
The additional keyword `WITH ORDINALITY` creates an output table
with two columns, where the second column is the index of the element
within the array, with numbering starting at 1.  If the array contains
duplicated values, the resulting table will be a multiset.

The `UNNEST` operator can be used in self-joins as follows:

```sql
SELECT city, country
FROM data, UNNEST(cities) AS t (city)
```

where we assume that the `data` table has a schema defined
as `CREATE TABLE data AS (CITIES VARCHAR ARRAY, COUNTRY VARCHAR)`.

## Predefined functions on array values


| Function                        | Description                                                                                     |Example|
|---------------------------------|-------------------------------------------------------------------------------------------------|-------|
| _array_`[`_index_`]`                | where _index_ is an expression that evaluates to an integer, and _array_ is an expression that evaluates to an array. Returns the element at the specified position. If the index is out of bounds, the result is `NULL`. | `ARRAY[2,3][2]` => 3 |
| `CARDINALITY(` _array_ `)`            | Returns the size of the _array_ expression (number of elements). | `CARDINALITY(ARRAY[2,3])` => 2 |
| `ELEMENT(` _array_ `)`                | Returns the single element of an _array_ of any type. If the array has zero elements, returns `NULL`. If the array has more than one element, it causes a runtime exception. | `ELEMENT(ARRAY[2])` => 2 |
| `ARRAY_TO_STRING(` _array_ `, ` _separator_ [`, ` _null_string_ ]`)` | Concatenates the values of the string array _array_, separated by the _separator_ string. If _null_string_ is given and is not `NULL`, then `NULL` array entries are represented by that string; otherwise, they are omitted. | `ARRAY_TO_STRING(ARRAY[1, 2, 3, NULL, 5], ',', '*')` => `1,2,3,*,5` |
| `ARRAY_JOIN` | Another name for `ARRAY_TO_STRING` | |

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


| Function                                                             | Description                                                                                                                                                                                                                                                                               | Example                                                             |
|----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| _array_`[`_index_`]`                                                 | where _index_ is an expression that evaluates to an integer, and _array_ is an expression that evaluates to an array. Returns the element at the specified position. If the index is out of bounds, the result is `NULL`.                                                                 | `ARRAY[2,3][2]` => 3                                                |
| `ARRAY_APPEND(` _array_ `,  ` _element_ `)`                          | Appends an element to the end of the array and returns the result. If the array is `NULL`, the function will return `NULL`. If the _element_ is `NULL`, the `NULL` element will be added to the end of the array.                                                                         | `ARRAY_APPEND(ARRAY [1, 2], 3)` => `[1, 2, 3]`                      |
| `ARRAY_COMPACT(` _array_ `)`                                         | Returns `NULL` if _array_ is `NULL`. Returns a new array by removing all the `NULL` values from the _array_.                                                                                                                                                                              | `ARRAY_COMPACT(ARRAY [1, 2, 3, NULL])` => `[1, 2, 3]`               |
| `ARRAY_CONTAINS(` _array_ `, ` _element_ `)`                         | Returns `NULL` if _element_ is `NULL`. Returns true if the _array_ contains the _element_.                                                                                                                                                                                                | `ARRAY_CONTAINS(ARRAY [1, 2, 3], 2)` => true                        |
| `ARRAY_DISTINCT(` _array_ `)`                                        | Returns `NULL` if _array_ is `NULL`. Returns a new array removing duplicate values from the _array_ and keeping the order of elements.                                                                                                                                                    | `ARRAY_DISTINCT(ARRAY [1, 1, 2, 2])` => `[1, 2]`                    |
| `ARRAY_JOIN`                                                         | Another name for `ARRAY_TO_STRING`                                                                                                                                                                                                                                                        |                                                                     |
| `ARRAY_LENGTH(` _array_ `)`                                          | Another name for `CARDINALITY`                                                                                                                                                                                                                                                            | `ARRAY_LENGTH( ARRAY [1, 2, 3])` => `3`                             |
| `ARRAY_MAX(` _array_ `)`                                             | Returns the maximum value in the _array_                                                                                                                                                                                                                                                  | `ARRAY_MAX(ARRAY [9, 1, 2, 4, 8, null])` => 9                       | 
| `ARRAY_MIN(` _array_ `)`                                             | Returns the minimum value in the _array_. Ignores null values.                                                                                                                                                                                                                            | `ARRAY_MIN(ARRAY [9, 1, 2, 5, 8, null])` => 1                       |
| `ARRAYS_OVERLAP(` _array1_ `, ` _array2_ `)`                         | Returns `NULL` if _array1_ or _array2_ is `NULL`. Errors if the two arrays are of different types. Returns true if the two arrays have at least one element in common. Returns `NULL` if they do not have elements in common but any of them has a `NULL` value. Returns false otherwise. | `ARRAYS_OVERLAP(ARRAY [1, 2, 3], ARRAY [3, 4, 5])` => true          |
| `ARRAY_POSITION(` _array_ `, ` _element_ `)`                         | Returns `NULL` if _array_ or _element_ is `NULL`. Returns the (1-based) index of the first _element_ of the _array_ as a long. Returns 0 if _element_ doesn't exist in _array_.                                                                                                           | `ARRAY_POSITION(ARRAY [1, 3, 4, 6], 4)` => 3                        |
| `ARRAY_PREPEND(` _array_ `,  ` _element_ `)`                         | Prepends an element to the start of the array and returns the result. If the array is `NULL`, the function will return `NULL`. If the element is `NULL`, the `NULL` element will be added to the start of the array.                                                                      | `ARRAY_PREPEND(ARRAY [2, 3], 1)` => `[1, 2, 3]`                     |
| `ARRAY_REMOVE(` _array_ `, ` _element_ `)`                           | Returns `NULL` if _array_ or _element_ is `NULL`. Returns a new array removing all elements that are equal to _element_ from the given _array_.                                                                                                                                           | `ARRAY_REMOVE(ARRAY [2, 2, 6, 8], 2)` => `[6, 8]`                   |
| `ARRAY_REPEAT(` _element_ `, ` _count_ `)`                           | Returns the array containing _element_ _count_ times. If _count_ is negative, an empty array is returned. If _count_ is `NULL`, `NULL` is returned. If _element_ is `NULL`, an array containing _count_ number of `NULL`s is returned.                                                    | `ARRAY_REPEAT(3, 4)` => `[3, 3, 3, 3]`                              |
| `ARRAY_SIZE(` _array_ `)`                                            | Another name for `CARDINALITY`                                                                                                                                                                                                                                                            | `ARRAY_SIZE( ARRAY [1, 2, 3])` => `3`                               |
| `ARRAY_TO_STRING(` _array_ `, ` _separator_ [`, ` _null_string_ ]`)` | Concatenates the values of the string array _array_, separated by the _separator_ string. If _null_string_ is given and is not `NULL`, then `NULL` array entries are represented by that string; otherwise, they are omitted.                                                             | `ARRAY_TO_STRING(ARRAY[1, 2, 3, NULL, 5], ',', '*')` => `1,2,3,*,5` |
| `CARDINALITY(` _array_ `)`                                           | Returns the size of the _array_ expression (number of elements).                                                                                                                                                                                                                          | `CARDINALITY(ARRAY[2,3])` => 2                                      |
| `ELEMENT(` _array_ `)`                                               | Returns the single element of an _array_ of any type. If the array has zero elements, returns `NULL`. If the array has more than one element, it causes a runtime exception.                                                                                                              | `ELEMENT(ARRAY[2])` => 2                                            |
| `SORT_ARRAY(` _array_ `, [` _ascendingOrder_ `])`                    | Returns a new array, sorted in ascending or descending order according to the natural ordering of the array elements. The default order is ascending if ascendingOrder is not specified. Null elements are considered to be the smallest.                                                 | `SORT_ARRAY(ARRAY [4, 7, 1, null])` => `[null, 1, 4, 7]`            | 

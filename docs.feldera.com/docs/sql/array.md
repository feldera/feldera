# Array Operations

An array type can be created by applying the `ARRAY` suffix to another
type.  For example `INT ARRAY` is an array of integers.  Array indexes
start from 1.  Array sizes are limited to 2^31 elements.  Array values
be nullable types, e.g., `INT ARRAY NULL`.  Multidimensional arrays
are possible, e.g. `VARCHAR ARRAY ARRAY` is a two-dimensional array.

In `CREATE TABLE` and `CREATE TYPE` declarations there is no way to
specify the nullability of the elements of an `ARRAY`.  The compiler
will always assume that array elements are nullable:

```sql
CREATE TABLE T(a INT ARRAY);
```

Table `T` will have a single column `a` whose values are nullable
arrays; the array elements will be nullable INT values.

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
CREATE TABLE data(CITIES VARCHAR ARRAY, COUNTRY VARCHAR);

CREATE VIEW V AS SELECT city, country
FROM data, UNNEST(cities) AS t (city);
```

The previous query is a shortcut for a CROSS-JOIN query:

```sql
CREATE VIEW V AS SELECT city, data.country
FROM data CROSS JOIN UNNEST(data.cities) AS city;
```

`UNNEST` applied to a `NULL` value returns an empty array.

Note that applying `UNNEST` to an `ARRAY` of structure-typed objects
will produce a collection whose columns are the fields of the
structure, instead of a collection with a single structure-typed
column.

## Comparison Operations on Arrays

Comparison operations (`=`, `<>`, `!=`, `>`, `<`, `>=`, `<=`) in arrays occur **lexicographically**, from left to right. The elements are compared one by one until a difference is found. Arrays can be compared even if they are different in size.

**Examples:**
- `[45] > [22]`
- `[23, 56, 16] > [23, NULL]`

## Predefined functions on array values


| Function                                                             | Description                                                                                                                                                                                                                                                                               | Example                                                             |
|----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| _array_`[`_index_`]`                                                 | where _index_ is an expression that evaluates to an integer, and _array_ is an expression that evaluates to an array. Returns the element at the specified position. If the index is out of bounds, the result is `NULL`.                                                                 | `ARRAY[2,3][2]` => 3                                                |
| _array_`[SAFE_OFFSET(`_index_`)]`                                    | where _index_ is an expression that evaluates to an integer, and _array_ is an expression that evaluates to an array. Returns the element at the specified position, with first element at index 0. If the index is out of bounds, the result is `NULL`.                                  | `ARRAY[2,3][2]` => NULL                                             |
| `ARRAY(` [ exp [, exp]* ]`)`                                         | Array constructor function.  Can have zero arguments.                                                                                               | `ARRAY()` => `[]`, `ARRAY(NULL, 2)` => `[NULL, 2]`                  |
| <a id="append"></a>`ARRAY_APPEND(` _array_ `,  ` _element_ `)`       | Appends an element to the end of the array and returns the result. If the array is `NULL`, the function will return `NULL`. If the _element_ is `NULL`, the `NULL` element will be added to the end of the array.                                                                         | `ARRAY_APPEND(ARRAY [1, 2], 3)` => `[1, 2, 3]`                      |
| <a id="concat"></a>`ARRAY_CONCAT(` _array_, [ _array_ ]* `)`         | Returns `NULL` if any argument is `NULL`. Concatenates a list of arrays.                                                                                                                                                                                                                  | `ARRAY_CONCAT(ARRAY [1, 2, 3], ARRAY[4])` => `[1, 2, 3, 4]`         |
| <a id="compact"></a>`ARRAY_COMPACT(` _array_ `)`                     | Returns `NULL` if _array_ is `NULL`. Returns a new array by removing all the `NULL` values from the _array_.                                                                                                                                                                              | `ARRAY_COMPACT(ARRAY [1, 2, 3, NULL])` => `[1, 2, 3]`               |
| <a id="contains"></a><a id="contains"></a>`ARRAY_CONTAINS(` _array_ `, ` _element_ `)` | Returns `NULL` if _element_ is `NULL`. The right argument must have the same type as array elements. Returns true if the _array_ contains the _element_.                                                                                                                | `ARRAY_CONTAINS(ARRAY [1, 2, 3], 2)` => true                        |
| <a id="distinct"></a>`ARRAY_DISTINCT(` _array_ `)`                   | Returns `NULL` if _array_ is `NULL`. Returns a new array removing duplicate values from the _array_ and keeping the order of elements.                                                                                                                                                    | `ARRAY_DISTINCT(ARRAY [1, 1, 2, 2])` => `[1, 2]`                    |
| <a id="except"></a>`ARRAY_EXCEPT(` _left_, _right_ `)`               | Returns `NULL` if any argument is `NULL`. Returns an array with all elements in the _left_ array that do not appear in the  _right_ array, removing duplicates.                                                                                                                           | `ARRAY_EXCEPT(ARRAY [1, 2, 3], ARRAY [3,1,4])` => `[2]`             |
| <a id="insert"></a>`ARRAY_INSERT(` _array_, _pos_, _element_ `)`     | Returns `NULL` if _array_ or _pos_ are `NULL`. Insert _element_ in array at specified position, padding with `NULL`s if necessary. If _pos_ is negative, it is considered from the end of the array. Produces error when _pos_ is zero or very large                                      | `ARRAY_INSERT(ARRAY [1, 2, 3], 3, 4)` => `[1,2,4,3]` |
| <a id="intersect"></a>`ARRAY_INTERSECT(` _left_, _right_ `)`         | Returns `NULL` if any argument is `NULL`. Returns an array with all elements that appear in common in both the _left_ array and _right_ arrays, with no duplicates.                                                                                                                       | `ARRAY_INTERSECT(ARRAY [1, 2, 3], ARRAY [3,1,4])` => `[1,3]`       |
| <a id="join"></a>`ARRAY_JOIN`                                        | Another name for `ARRAY_TO_STRING`                                                                                                                                                                                                                                                        |                                                                     |
| <a id="length"></a>`ARRAY_LENGTH(` _array_ `)`                       | Another name for `CARDINALITY`                                                                                                                                                                                                                                                            | `ARRAY_LENGTH( ARRAY [1, 2, 3])` => `3`                             |
| <a id="max"></a>`ARRAY_MAX(` _array_ `)`                             | Returns the maximum value in the _array_                                                                                                                                                                                                                                                  | `ARRAY_MAX(ARRAY [9, 1, 2, 4, 8, null])` => 9                       |
| <a id="min"></a>`ARRAY_MIN(` _array_ `)`                             | Returns the minimum value in the _array_. Ignores null values.                                                                                                                                                                                                                            | `ARRAY_MIN(ARRAY [9, 1, 2, 5, 8, null])` => 1                       |
| <a id="overlap"></a>`ARRAYS_OVERLAP(` _array1_ `, ` _array2_ `)`     | Returns `NULL` if _array1_ or _array2_ is `NULL`. Errors if the two arrays are of different types. Returns true if the two arrays have at least one element in common.                                                                                                                    | `ARRAYS_OVERLAP(ARRAY [1, 2, 3], ARRAY [3, 4, 5])` => true          |
| <a id="position"></a>`ARRAY_POSITION(` _array_ `, ` _element_ `)`    | Returns `NULL` if _array_ or _element_ is `NULL`. The right argument must have the same type as array elements. Returns the (1-based) index of the first _element_ of the _array_ as a long. Returns 0 if _element_ doesn't exist in _array_.                                             | `ARRAY_POSITION(ARRAY [1, 3, 4, 6], 4)` => 3                        |
| <a id="prepend"></a>`ARRAY_PREPEND(` _array_ `,  ` _element_ `)`     | Prepends an element to the start of the array and returns the result. If the array is `NULL`, the function will return `NULL`. If the element is `NULL`, the `NULL` element will be added to the start of the array.                                                                      | `ARRAY_PREPEND(ARRAY [2, 3], 1)` => `[1, 2, 3]`                     |
| <a id="remove"></a>`ARRAY_REMOVE(` _array_ `, ` _element_ `)`        | Returns `NULL` if _array_ or _element_ is `NULL`. The right argument must have the same type as array elements. Returns a new array removing all elements that are equal to _element_ from the given _array_.                                                                             | `ARRAY_REMOVE(ARRAY [2, 2, 6, 8], 2)` => `[6, 8]`                   |
| <a id="reverse"></a>`ARRAY_REVERSE(` _array_ `)`                     | Returns `NULL` if _array_ is `NULL`. Returns array with the elements in reverse order.                                                                                                                                                                                                    | `ARRAY_REVERSE(ARRAY [2, 2, 6, 8])` => `[8, 6, 2, 2]`               |
| <a id="repeat"></a>`ARRAY_REPEAT(` _element_ `, ` _count_ `)`        | Returns the array containing _element_ _count_ times. If _count_ is negative, an empty array is returned. If _count_ is `NULL`, `NULL` is returned. If _element_ is `NULL`, an array containing _count_ number of `NULL`s is returned.                                                    | `ARRAY_REPEAT(3, 4)` => `[3, 3, 3, 3]`                              |
| <a id="size"></a>`ARRAY_SIZE(` _array_ `)`                           | Another name for `CARDINALITY`                                                                                                                                                                                                                                                            | `ARRAY_SIZE( ARRAY [1, 2, 3])` => `3`                               |
| <a id="to_string"></a>`ARRAY_TO_STRING(` _array_ `, ` _separator_ [`, ` _null_string_ ]`)` | Concatenates the values of the string array _array_, separated by the _separator_ string. If _null_string_ is given and is not `NULL`, then `NULL` array entries are represented by that string; otherwise, they are omitted.                                                             | `ARRAY_TO_STRING(ARRAY[1, 2, 3, NULL, 5], ',', '*')` => `1,2,3,*,5` |
| <a id="union"></a>`ARRAY_UNION(` _left_, _right_ `)`                 | Returns `NULL` if any argument is `NULL`. Returns an array with all elements that either in the _left_ array or the _right_ arrays, with no duplicates.                                                                                                                                   | `ARRAY_UNION(ARRAY [1, 2, 3], ARRAY [3,1,4])` => `[1,2,3,4]`        |
| <a id="cardinality"></a>`CARDINALITY(` _array_ `)`                   | Returns the size of the _array_ expression (number of elements).                                                                                                                                                                                                                          | `CARDINALITY(ARRAY[2,3])` => 2                                      |
| <a id="element"></a>`ELEMENT(` _array_ `)`                           | Returns the single element of an _array_ of any type. If the array has zero elements, returns `NULL`. If the array has more than one element, it causes a runtime exception.                                                                                                              | `ELEMENT(ARRAY[2])` => 2                                            |
| <a id="sort"></a>`SORT_ARRAY(` _array_ `, [` _ascendingOrder_ `])`   | Returns a new array, sorted in ascending or descending order according to the natural ordering of the array elements. The default order is ascending if ascendingOrder is not specified. Null elements are considered to be the smallest.                                                 | `SORT_ARRAY(ARRAY [4, 7, 1, null])` => `[null, 1, 4, 7]`            |

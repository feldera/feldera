# Mutually-recursive queries in Feldera SQL

::: warning

Support for recursive queries is a new feature, which is still
evolving.  Details may change.

:::

Standard SQL has very limited support for recursive computations using
common-table expressions.  The standard definition is both difficult
to use, and lacks in expressivity.  Feldera SQL uses an alternative
syntax for defining recursive SQL computations, which is easier
to use and significantly more powerful.  Our syntax:

- does not require common-table expressions

- allows mutually-recursive view definitions involving any number of
  views

- allows much richer queries for defining recursive views, including
  non-monotone queries.  We eventually aim to support arbitrary
  queries in recursive views.

With these changes our query language becomes Turing-complete.  As a
consequence, it is possible to write recursive queries whose
evaluation never terminates.

## Defining mutually-recursive views

### Forward declarations

Our syntax for mutually-recursive queries is inspired from the C
programming language.  In C one writes recursive functions by usign
*forward declarations*.  In a C program the forward declarations have
to occur *before* the actual function definitions.

For example, to write functions `f` and `g` that invoke each other one
first writes the declaration(s):

```C
/* Forward declaration of function g */
int g(int x);

/* Definition of function f */
int f(int x) {
  ... g(n-1); /* f calling g */
}

/* Definition of function g */
int g(int x) {
  ... f(n-1); /* g calling f */
}
```

Similarly to C, we introduce the notion of *forward view declaration*.
A forward view declaration looks like a table declaration (but without
constraints on columns).  The declaration specifies the view name and
the columns and their types, as in the following example:

```sql
CREATE RECURSIVE VIEW CLOSURE(x int, y int);
```

Each view that that is used in a query before being defined needs a
forward declaration.  The forward declaration must occur before the
view definition in the program.

### Recursive View definitions

Finally, given a forward declaration, a recursive view can be defined
using the standard SQL syntax.  The forward declaration allows the
view query to depend on the view itself:

```sql
CREATE TABLE EDGES(x int, y int);

-- a local view that depends on CLOSURE and EDGES
CREATE LOCAL VIEW STEP AS
SELECT E.x, CLOSURE.y FROM
E JOIN CLOSURE ON e.y = CLOSURE.x

-- actual definition of view CLOSURE, which depends on STEP and E
CREATE MATERIALIZED VIEW CLOSURE AS
(SELECT * FROM EDGES) UNION (SELECT * FROM STEP);
```

In the above SQL example the `EDGES` table represents the edges of a
graph where nodes are integers.  The `CLOSURE` view represents the
edges of a graph over the same nodes, such that `CLOSURE` is the
transitive closure of the `EDGES` graph.

Notice that view `STEP` does not need a forward declaration.  However,
if view `STEP` is not a `LOCAL` view, then it does need a forward
declaration -- all views that are outputs of the SQL pipeline that are
mutually recursive require forward declarations.

The type inferred by the compiler for the view based on the query must
match exactly the type of the declared view, including the nullability
of the columns, and the column names.

## Semantics

Currently the compiler automatically makes all mutually-recursive
views `DISTINCT` (they cannot contain duplicates), as if they have a
`SELECT DISTINCT` in their definition.  Without this restrictions,
(and if we replace `UNION` with `UNION ALL` in the view definition),
the program that computes `CLOSURE` abaove would never terminate,
since the `CLOSURE` table would keep growing, accumulating duplicate
edges.

The semantics of mutually-recursive views matches the semantics from
other programming languages: for any contents of the input tables, the
views initially start empty, and then the queries for the views are
evaluated until the views stop changing.  Thus a set of mutually
recursive views computes the least fixpoint of the queries.

(Note that this is *not* the semantics that SQL gives to recursive
queries.)

We can verify this using our previous program and using the [Feldera
Shell](https://docs.feldera.com/reference/cli)

```sql
INSERT INTO EDGES VALUES(0, 1);
SELECT * FROM CLOSURE;
 x | y
-------
 0 | 1
(1 row)
INSERT INTO EDGES VALUES(1, 2);
SELECT * FROM CLOSURE;
 x | y
-------
 0 | 1
 1 | 2
 0 | 2
(3 rows)
DELETE * FROM EDGES WHERE x = 0;
SELECT * FROM CLOSURE;
 x | y
-------
 1 | 2
(1 row)
```

## Restrictions

Feldera supports a much wider range of operators in recursive queries
than other SQL dialects.  Currently the following operators *can* be
used in recursive queries: `SELECT`, `WHERE`, `GROUP BY`, all kinds of
`JOIN`s, `HAVING`, aggregations, `DISTINCT`, `UNNEST`, `VALUES`,
`ORDER BY`, `PIVOT`, `UNION`, `INTERSECT`, `EXCEPT`, table functions
such as `HOP` and `TUMBLE`.

Unsupported constructs include window functions (using `OVER`).

Programs can mix annotations for [streaming computations](streaming)
and recursive computations, but currently the garbage-collection
mechanisms are ineffective for recursive views.

It is easy to write programs that will run for a very long time, e.g.,
the following view will attempt to create a view that contains all
integer legal values:

```sql
CREATE RECURSIVE VIEW V(x INT);
CREATE VIEW V as SELECT x+1 FROM V UNION (SELECT 1);
```
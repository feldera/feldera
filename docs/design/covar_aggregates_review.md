# Review of the COVAR_* and REGR_* aggregates

Findings from testing the covariance family added by
"[SQL] Support for COVAR_* and REGR_* aggregate functions" (PR #6812).
Every expected value below was validated on Postgres 14.

The tests that pin these findings are in
`sql-to-dbsp-compiler/SQL-compiler/src/test/java/org/dbsp/sqlCompiler/compiler/sql/simple/`:

| File | Cases | Status at the tip of this branch |
| --- | --- | --- |
| `CovarAdversarialTests.java` | 9 | 5 pass, 4 fail on findings 3 and 4 |
| `CovarIncrementalTests.java` | 2 | both pass |

Against the PR alone, 8 of the 9 adversarial cases fail. Findings 1 and 2 are
fixed by the two commits that follow the tests on this branch; findings 3 and 4
are open.

## Summary

| Case | Feldera | Postgres | Introduced by the PR | Status |
| --- | --- | --- | --- | --- |
| `COVAR_POP(y NOT NULL, x)` with `GROUP BY`, group with no valid pair | panic: `NULL value should be impossible here` | `NULL` | yes | fixed here |
| the same, as `OVER (PARTITION BY k)` | the same panic | `NULL` | yes | fixed here |
| `COVAR_POP(int, double)` against `COVAR_POP(double, int)` | `0` against `12.5` | `12.5` both | yes | fixed here |
| `COVAR_POP(DECIMAL(6,4), DECIMAL(12,2))` | panic: `Cannot represent 1000000 as DECIMAL(18, 12)` | `50` | yes | fixed here |
| `REGR_SXX(v, v)` over `INT`, value 5e9 | panic: `Error converting 5000000000 to INTEGER` | `5000000000` | yes | open |
| `REGR_SXX(units, price)` over `DECIMAL(7,2)` | panic: `Cannot represent 2000000 as DECIMAL(7, 2)` | `2000000` | yes | open |
| `COVAR_POP(v, v)` over two `INT` rows of 2e9 | panic: `'4000000000 * 4000000000' causes overflow for type BIGINT` | `0` | no, `VAR_POP` behaves the same | open |

## Finding 1: a NULL result is declared NOT NULL, so the pipeline panics

```sql
CREATE TABLE T(k INT, y INT NOT NULL, x INT);
INSERT INTO T VALUES (1, 5, NULL);
SELECT k, COVAR_POP(y, x) FROM T GROUP BY k;
```

Postgres returns NULL. Feldera panics with `NULL value should be impossible here`.

Calcite's `ReturnTypes.COVAR_REGR_FUNCTION` makes the result nullable only for an
empty group, a `FILTER`, or `COVAR_SAMP`. It misses the case that this family
introduces: the result is also NULL when no row has both arguments non-NULL.
With `y` declared NOT NULL and a `GROUP BY`, the view column is typed `i32`, and
the generated code reduces the aggregate through

```rust
handle_error_with_position(..., unwrap_value(..., "NULL value should be impossible here"))
```

which panics rather than producing a row. A query without `GROUP BY` escapes only
because `hasEmptyGroup()` already forces nullability. `VAR_POP` is unaffected,
since a non-nullable argument over a non-empty group cannot produce NULL.

The window form of the aggregate, which the convertlet in `ConvertletTable.java`
expands instead, panics the same way.

Test: `CovarAdversarialTests.nonNullableFirstArgument`,
`CovarAdversarialTests.nonNullableFirstArgumentWindow`.

## Finding 2: the second argument is coerced into the type of the first

```sql
CREATE TABLE T(y INT, x DOUBLE);
INSERT INTO T VALUES (0, 0.1), (100, 0.2), (200, 0.3), (300, 0.4);
SELECT COVAR_POP(y, x), COVAR_POP(x, y) FROM T;
```

Postgres returns 12.5 twice. Feldera returns 0 and 12.5, with no error.
`REGR_SXX(y, x)` and `REGR_SYY(x, y)` denote the same quantity and return 0 and
0.05.

`partialResultType` derives from the result type, which Calcite takes from the
first argument (`AggregateCompiler.java:162`, `computePartialResultType` at
`AggregateCompiler.java:181`). `doCovariance` then casts both arguments to it
(`AggregateCompiler.java:1091` for the linear implementation and
`AggregateCompiler.java:1129` for the non-linear one). Here `x` rounds to `i64`,
every value becomes 0, and the answer is silently 0. Covariance is symmetric, so
a result that depends on argument order is self-inconsistent.

Silence makes this the most dangerous of the findings. The same cause is loud
when both arguments are DECIMAL of different precision: widening `DECIMAL(6,4)`
gives `DECIMAL(18,12)`, which holds six integer digits, so an amount of 1000000
in the second argument panics on the cast.

Test: `CovarAdversarialTests.symmetry`,
`CovarAdversarialTests.argumentsOfDifferentPrecision`.

## Finding 3: REGR_SXX and REGR_SYY do not fit in the type of their arguments

```sql
CREATE TABLE T(id INT, v INT);
INSERT INTO T VALUES (1, 100000), (2, 200000);
SELECT REGR_SXX(v, v) FROM T;                    -- 5000000000 on Postgres

CREATE TABLE S(id INT, price DECIMAL(7,2), units DECIMAL(7,2));
INSERT INTO S VALUES (1, 1000.00, 3.00), (2, 2000.00, 5.00), (3, 3000.00, 4.00);
SELECT REGR_SXX(units, price) FROM S;            -- 2000000 on Postgres
```

Both panic, the first with `Error converting 5000000000 to INTEGER`, the second
with `Cannot represent 2000000 as DECIMAL(7, 2)`.

`REGR_SXX` is `REGR_COUNT * VAR_POP`, so it outgrows its arguments by design.
Returning it in the type of the first argument makes the failure routine rather
than exceptional. The PR already works around this in its own test:
`AggScottTests.testRegrValue` wraps every argument in `CAST(... AS DECIMAL(12,4))`
because "the result, which sums squared values, overflows DECIMAL(7,2)". A user
who hits the same limit gets a runtime panic instead of a compilation error, and
`docs.feldera.com/docs/sql/aggregates.md` does not warn about it.

Test: `CovarAdversarialTests.sumOfSquaresOutgrowsIntegerArgument`,
`CovarAdversarialTests.sumOfSquaresOutgrowsDecimalArgument`.

## Finding 4: the accumulator overflows on ordinary values

```sql
CREATE TABLE T(id INT, v INT);
INSERT INTO T VALUES (1, 2000000000), (2, 2000000000);
SELECT COVAR_POP(v, v) FROM T;
```

The covariance of a constant column is 0. Feldera panics with
`'4000000000 * 4000000000' causes overflow for type BIGINT`, raised by
`sumA * sumB` in `AggregateCompiler.covariance` at `AggregateCompiler.java:1030`,
which computes in `i64` for every integer argument type.

This one predates the PR: `VAR_POP` over the same column fails identically, and
the PR leaves that arithmetic alone (`AggregateCompiler.java:920` only drops a
redundant cast). It is listed here because five more functions now inherit it,
and because `REGR_SXX` and `REGR_SYY` reach the limit `n` times sooner than
`VAR_POP` does.

Tests: `CovarAdversarialTests.covarOfConstantColumn` and its control
`CovarAdversarialTests.varPopOfConstantColumn`, which fails the same way and
shows the defect is not new.

## What holds up

- Incremental correctness. Deleting rows walks the results back through the
  values that inserting them produced: a group shrinking to one pair turns
  `COVAR_SAMP` NULL, shrinking to no pair turns everything but `REGR_COUNT` NULL,
  and emptying the table produces no spurious delta. Groups that appear and
  disappear are also correct. `CovarIncrementalTests` covers both.
- The window form agrees with the grouped form on the cases tried, including
  data where only some rows have both arguments non-NULL.
- `REGR_COUNT`, `FILTER`, and pair-wise NULL skipping match Postgres.
- The new `QuadSemigroup` and the non-linear DOUBLE implementation are correct on
  everything tried here.

## The fix for findings 1 and 2

Both come from one Calcite hook. `RelDataTypeSystemImpl.deriveCovarType` returns
the type of the first argument, unchanged; overriding it in
`SqlToRelCompiler.TYPE_SYSTEM` (`SqlToRelCompiler.java:418`) addresses both, and
the two commits that follow the tests on this branch do exactly that, one finding
each:

```java
@Override
public RelDataType deriveCovarType(RelDataTypeFactory typeFactory,
                                   RelDataType arg0Type, RelDataType arg1Type) {
    RelDataType common = typeFactory.leastRestrictive(List.of(arg0Type, arg1Type));
    if (common == null)
        common = arg0Type;
    return typeFactory.createTypeWithNullability(common, true);
}
```

`createTypeWithNullability` fixes finding 1, and `leastRestrictive` fixes finding
2. `COVAR_REGR_FUNCTION` returns what this hook produces, and the paths that
already force nullability, an empty group, a `FILTER`, and `COVAR_SAMP`, keep
working.

Measured: `nonNullableFirstArgument`, `nonNullableFirstArgumentWindow`,
`symmetry`, and `argumentsOfDifferentPrecision` pass, and `CovarTests`,
`AggScottTests.testRegrValue`, and `WinAggPostTests.testWindowCovariance` still
pass, so arguments of the same type keep the results the PR gives them.

## The open findings

Findings 3 and 4 need a wider computation type, not only a wider result type.
Two options:

- Return DOUBLE, as Postgres does. This removes both overflow classes. It costs
  the exact DECIMAL results and requires updating the expected values in
  `CovarTests` and `AggScottTests`.
- Keep the declared type and widen only the intermediate: `i128` for integer
  arguments and `DECIMAL(38, s)` for decimal ones. Cheaper, but the final
  narrowing cast can still fail.

Either way, `docs.feldera.com/docs/sql/aggregates.md` should also state that a
value exceeding the result type is a runtime error. It documents the type of the
result, but reads as though that choice were harmless.

## Running the tests

```
cd sql-to-dbsp-compiler/SQL-compiler
mvn test -Dtest=CovarAdversarialTests -DargLine="-ea"
mvn test -Dtest=CovarIncrementalTests -DargLine="-ea"
```

A value mismatch is reported by the Java test. A panic surfaces as
`Process failed with exit code 101` from the Rust run at the end of the class; the
message is in the output of `cargo test` in `sql-to-dbsp-compiler/temp`.

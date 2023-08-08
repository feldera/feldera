# Date/time operations

`TIME` literals have the form `TIME 'HH:MM:SS.FFF`, where the
fractional part is optional, and can have between 0 and 9 digits.  A
time represents a time within a day, limited to between 0 and 24
hours.

Values of type `DATE`, `TIME`, and `TIMESTAMP` can be compared
using `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`, `<=>`,
`BETWEEN`; the result is a Boolean.

The following arithmetic operations are supported:

| Operation                       | Result Type | Explanation                                               |
|---------------------------------|--------------------|----------------------------------------------------|
| `DATE` + `INTEGER`              | `DATE`             | Add a number of days to a date                     |
| `DATE` + `INTERVAL`             | `TIMESTAMP`        | Add an interval to a date                          |
| `DATE` + `TIME`                 | `TIMESTAMP`        | Create a timestamp from parts                      |
| `INTERVAL` + `INTERVAL`         | `INTERVAL`         | Add two intervals; both must have the same type    |
| `TIMESTAMP` + `INTERVAL`        | `TIMESTAMP`        | Add an interval to a timestamp                     |
| `TIME` + `INTERVAL` (short)     | `TIME`             | Add an interval to a time                          |
| `-` `INTERVAL`                  | `INTERVAL`         | Negate an interval                                 |
| `DATE` - `DATE`                 | `INTERVAL`         | Compute the interval between two dates             |
| `DATE` - `INTEGER`              | `DATE`             | Subtract a number of days from a date              |
| `DATE` - `INTERVAL`             | `DATE`             | Subtract an interval from a date                   |
| `TIME` - `TIME`                 | `INTERVAL` (short) | Compute the difference between two times           |
| `TIME` - `INTERVAL` (short)     | `TIME`             | Subtract an interval from a time                   |
| `TIMESTAMP` - `INTERVAL`        | `TIMESTAMP`        | Subtract an interval from a timestamp              |
| `INTERVAL` - `INTERVAL`         | `INTERVAL`         | Subtract two intervals                             |
| `INTERVAL` * `DOUBLE`           | `INTERVAL`         | Multiply an interval by a scalar                   |
| `INTERVAL` / `DOUBLE`           | `INTERVAL`         | Divide an interval by a scalar                     |
| `TIMESTAMP` - `TIMESTAMP`       | `INTERVAL` (long)  | Subtract two timestamps, convert result into days  |

The following are legal time units:

| Time unit       | Meaning                                                                                |
|-----------------|----------------------------------------------------------------------------------------|
| `MILLENNIUM`    | A thousand years                                                                       |
| `CENTURY`       | A hundred years; a number between 1 and 10                                             |
| `DECADE`        | Ten years; a number between 1 and 10                                                   |
| `YEAR`          | One year; can be positive or negative                                                  |
| `QUARTER`       | 1/4 of a year; a number between 1 and 4                                                 |
| `MONTH`         | One month; a number between 1 and 12                                                    |
| `WEEK`          | Seven days. The definition of "week" is quite involved: The year's first week is the week containing the first Thursday of the year or either the week containing the 4th of January or either the week that begins between 29th of Dec. and 4th of Jan. The week number is thus a value between 0 and 53. |
| `DOY`           | Day of year, a number between 1 and 366                                                 |
| `DOW`           | Day of week, with Sunday being 1 and Saturday being 7                                   |
| `ISODOW`        | ISO day of the week, with Monday 1 and Sunday 7                                         |
| `DAY`           | A day within a month, a number between 1 and 31                                         |
| `HOUR`          | An hour within a day, a number between 0 and 23                                         |
| `MINUTE`        | A minute within an hour, a number between 0 and 59                                      |
| `SECOND`        | A second within a minute, a number between 0 and 59                                     |
| `MILLISECOND`   | A millisecond within a *minute*, including the number of seconds multiplied by 1000, a number between 0 and 59,999                                                                                      |
| `MICROSECOND`   | A microsecond within a *minute*, including the number of seconds multiplied by 1,000,000, a number between 0 and 59,999,999                                                                                    |
| `EPOCH`         | Number of seconds from Unix epoch, i.e., 1970/01/01.                                    |


## Operations on dates

`DATE` literals have the form `DATE 'YYYY-MM-DD'`.

Date literals can only represent 4-digit year positive values.
Values BC or values greater than 10,000 years are not supported.

The following operations are available on dates:

`EXTRACT(<unit> FROM date)` where `<unit>` is a time unit, as
described above.  Result is always a `BIGINT` value.
`DATE_PART` is a synonym for `EXTRACT`.

The following abbreviations can be used as well:

`YEAR(date)` is an abbreviation for `EXTRACT(YEAR FROM date)`.

`MONTH(date)` is an abbreviation for `EXTRACT(MONTH FROM date)`.

`DAYOFMONTH(date)` is an abbreviation for `EXTRACT(DAY FROM
date)`.

`DAYOFWEEK(date)` is an abbreviation for `EXTRACT(DOW FROM
date)`.

`HOUR(date)` is an abbreviation for `EXTRACT(HOUR FROM date)`.
For dates it always returns 0, since dates have no time component.

`MINUTE(date)` is an abbreviation for `EXTRACT(MINUTE FROM date)`.
For dates it always returns 0, since dates have no time component.

`SECOND(date)` is an abbreviation for `EXTRACT(SECOND FROM date)`.
For dates it always returns 0, since dates have no time component.

`FLOOR(datetime TO <unit>)`, where `<unit>` is a time unit.

`CEIL(datetime TO <unit>)`, where `<unit>` is a time unit.

## Operations on timestamps

A timestamp contains both a date and a time.  `TIMESTAMP` literals
have the form `TIMESTAMP 'YYYY-MM-DD HH:MM:SS.FFF'`, where the
fractional part is optional.

Timestamp literals can only represent 4-digit year positive values.
Values BC or values greater than 10,000 years are not supported.

The precision of timestamps is milliseconds, additional digits of
precision in timestamp literals are rounded to milliseconds.

The following operations are available on timestamps:

`EXTRACT(<unit> FROM timestamp)` where `<unit>` is a time unit, as
described above.  Result is always a `BIGINT` value.

The following abbreviations can be used as well:

`YEAR(timestamp)` is an abbreviation for `EXTRACT(YEAR FROM timestamp)`.

`MONTH(timestamp)` is an abbreviation for `EXTRACT(MONTH FROM timestamp)`.

`DAYOFMONTH(timestamp)` is an abbreviation for `EXTRACT(DAY FROM
timestamp)`.

`DAYOFWEEK(timestamp)` is an abbreviation for `EXTRACT(DOW FROM
timestamp)`.

`HOUR(timestamp)` is an abbreviation for `EXTRACT(HOUR FROM timestamp)`.

`MINUTE(timestamp)` is an abbreviation for `EXTRACT(MINUTE FROM timestamp)`.

`SECOND(timestamp)` is an abbreviation for `EXTRACT(SECOND FROM
timestamp)`.

`TIMESTAMPDIFF(<unit>, left, right)` computes the difference between
two timestamps and expresses the result in the specified time units.
The result is a 32-bit integer.

## Time intervals

### The interval types

Note that currently one cannot specify a type of `INTERVAL` for a
table column.  Interval types can be generated by queries though, so
they can appear in the computed views.

### Interval literals

Interval literals (constant values) can be written using the following
verbose syntax:

```
INTERVAL 'string' timeUnit [ TO timeUnit]
```

`tiemUnit` is one of `millisecond`, `second`, `minute`, `hour`, `day`,
`week`, `month`, `quarter`, `year`, or plurals of these units.  Only
the following combinations are supported:

|Type|Example literal|
|----|---------------|
|`INTERVAL YEAR`             | `INTERVAL '2021' YEAR`                          |
|`INTERVAL YEAR TO MONTH`    | `INTERVAL '2021-07' YEAR TO MONTH`              |
|`INTERVAL MONTH`            | `INTERVAL '10' MONTH`                           |
|`INTERVAL DAY`              | `INTERVAL '100' DAY`                            |
|`INTERVAL DAY TO HOUR`      | `INTERVAL '100 10' DAY TO HOUR`                 |
|`INTERVAL DAY TO MINUTE`    | `INTERVAL '100 10:30' DAY TO MINUTE`            |
|`INTERVAL DAY TO SECOND`    | `INTERVAL '100 10:30:40.999999' DAY TO SECOND`  |
|`INTERVAL HOUR`             | `INTERVAL '123' HOUR`                           |
|`INTERVAL HOUR TO MINUTE`   | `INTERVAL '123:10' HOUR TO MINUTE`              |
|`INTERVAL HOUR TO SECOND`   | `INTERVAL '123:10:59' HOUR TO SECOND`           |
|`INTERVAL MINUTE`           | `INTERVAL '1000' MINUTE`                        |
|`INTERVAL MINUTE TO SECOND` | `INTERVAL '1000:01.001' MINUTE TO SECOND`       |
|`INTERVAL SECOND`           | `INTERVAL '1000.000001' SECOND`                 |


A leading negative sign applies to all fields; for example the
negative sign in the interval literal `INTERVAL '-1 2:03:04' DAYS TO
SECONDS` applies to both the days and hour/minute/second parts.

## Timezones

`DATE`, `TIME` and `TIMESTAMP` have no time zone. For those types,
there is not even an implicit time zone, such as UTC (as in Java) or
the local time zone.  It is left to the user or application to supply a
time zone.

## Important unsupported operations

Since DBSP is a *deterministic* query engine, it cannot offer support
for any function that depends on the current time.  So the following
are *not* supported: `LOCALTIME`, `LOCALTIMESTAMP`,
`CURRENT_TIME`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`.

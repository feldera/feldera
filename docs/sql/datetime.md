# Date/time/time intervals

## Time units

The following are legal time units:

| Time unit         | Meaning                                                                                                                                                                                                                                                                                                    |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `MILLENNIUM`      | A thousand years                                                                                                                                                                                                                                                                                           |
| `CENTURY`         | A hundred years; a number between 1 and 10                                                                                                                                                                                                                                                                 |
| `DECADE`          | Ten years; a number between 1 and 10                                                                                                                                                                                                                                                                       |
| `YEAR`            | One year; can be positive or negative                                                                                                                                                                                                                                                                      |
| `QUARTER`         | 1/4 of a year; a number between 1 and 4                                                                                                                                                                                                                                                                    |
| `MONTH`           | One month; a number between 1 and 12                                                                                                                                                                                                                                                                       |
| `WEEK`            | Seven days. The definition of "week" is quite involved: The year's first week is the week containing the first Thursday of the year or either the week containing the 4th of January or either the week that begins between 29th of Dec. and 4th of Jan. The week number is thus a value between 0 and 53. |
| `DOY`             | Day of year, a number between 1 and 366                                                                                                                                                                                                                                                                    |
| `DOW`             | Day of week, with Sunday being 1 and Saturday being 7                                                                                                                                                                                                                                                      |
| `ISODOW`          | ISO day of the week, with Monday 1 and Sunday 7                                                                                                                                                                                                                                                            |
| `DAY`             | A day within a month, a number between 1 and 31                                                                                                                                                                                                                                                            |
| `HOUR`            | An hour within a day, a number between 0 and 23                                                                                                                                                                                                                                                            |
| `MINUTE`          | A minute within an hour, a number between 0 and 59                                                                                                                                                                                                                                                         |
| `SECOND`          | A second within a minute, a number between 0 and 59                                                                                                                                                                                                                                                        |
| `MILLISECOND`     | A millisecond within a *minute*, including the number of seconds multiplied by 1000, a number between 0 and 59,999                                                                                                                                                                                         |
| `MICROSECOND`     | A microsecond within a *minute*, including the number of seconds multiplied by 1,000,000, a number between 0 and 59,999,999                                                                                                                                                                                |
| `EPOCH`           | Number of seconds from Unix epoch, i.e., 1970/01/01.                                                                                                                                                                                                                                                       |
| `SQL_TSI_YEAR`    | Same as `YEAR`                                                                                                                                                                                                                                                                                             |
| `SQL_TSI_QUARTER` | Same as `QUARTER`                                                                                                                                                                                                                                                                                          |
| `SQL_TSI_MONTH`   | Same as `MONTH`                                                                                                                                                                                                                                                                                            |
| `SQL_TSI_WEEK`    | Same as `WEEK`                                                                                                                                                                                                                                                                                             |
| `SQL_TSI_HOUR`    | Same as `HOUR`                                                                                                                                                                                                                                                                                             |
| `SQL_TSI_DAY`     | Same as `DAY`                                                                                                                                                                                                                                                                                              |
| `SQL_TSI_MINUTE`  | Same as `MINUTE`                                                                                                                                                                                                                                                                                           |
| `SQL_TSI_SECOND`  | Same as `SECOND`                                                                                                                                                                                                                                                                                           |

## Dates

The date type represents a Gregorian calendar date, independent of
time zone.  This extends to dates before the Gregorian calendar was
introduced, effectively meaning that the dates use a <a
href="https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar">Proleptic
calendar</a>.

### Date literals

`DATE` literals have the form `DATE 'YYYY-MM-DD'`.  Trailing spaces
are not allowed.

Date literals can only represent 4-digit year positive values.
Values BC or values greater than 10,000 years are not supported.

### Date operations

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

Values of type `DATE` can be compared using `=`, `<>`, `!=`, `<`, `>`,
`<=`, `>=`, `<=>`, `BETWEEN`; the result is a Boolean.

## Times

A time represents the time of day, a value between 0 and 24 hours
(excluding the latter).

The `TIME` data type can specify an optional precision, e.g.,
`TIME(2)`.  The precision is the number of sub-second digits
supported.  So `TIME(3)` is a time with a precision of milliseconds.

The default precision is 3.

Currently the maximum supported precision is 3 (milliseconds).  Larger
precisions are accepted, but internally only up to 3 digits of
precision are maintained.

### Time literals

`TIME` literals have the form `TIME 'HH:MM:SS.FFF'`, where the
fractional part is optional, and can have between 0 and 3 digits.  An
example is: '23:59:59.132'.  The hours must be between 0 and 23, the
minutes between 0 and 59, and the seconds between 0 and 59.  Exactly
two digits must be used for hours, minutes, and seconds.  Spaces are
not allowed between quotes.

### Time operations

`EXTRACT(<unit> FROM time)` where `<unit>` is a time unit from
`HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`; the semantics is as
described above.  Result is always a `BIGINT` value.

The following abbreviations can be used as well:

`HOUR(time)` is an abbreviation for `EXTRACT(HOUR FROM time)`.

`MINUTE(time)` is an abbreviation for `EXTRACT(MINUTE FROM time)`.

`SECOND(time)` is an abbreviation for `EXTRACT(SECOND FROM
time)`.

Values of type `TIME` can be compared using `=`, `<>`, `!=`, `<`, `>`,
`<=`, `>=`, `<=>`, `BETWEEN`; the result is a Boolean.

## Timestamps

The `TIMESTAMP` data type represents values composed of a `DATE` (as
described above) and a `TIME`.  `TIMESTAMP` support an optional
precision specification, e.g.: `TIMESTAMP(3)`.  The precision applies
to the `TIME` component of the `TIMESTAMP`.  The maximum precision
supported for timestamps is 3.  The default precision for timestamps
(used when no precision is specified), is also 3.

### Timestamp literals

`TIMESTAMP` literals have the form `TIMESTAMP 'YYYY-MM-DD
HH:MM:SS.FFF'`, where the fractional part is optional.  Trailing
spaces are not allowed.

Timestamp literals can only represent 4-digit year positive values.
Values BC or values greater than 10,000 years are not supported.

The following operations are available on timestamps:

### Operations on timestamps

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

Values of type `TIMESTAMP` can be compared using `=`, `<>`, `!=`, `<`,
`>`, `<=`, `>=`, `<=>`, `BETWEEN`; the result is a Boolean.

`TIMESTAMPDIFF(<unit>, left, right)` computes the difference between
two timestamps and expresses the result in the specified time units.
The result is a 32-bit integer.  `DATEDIFF` is a synonym for
`TIMESTAMPDIFF`.  One month is considered elapsed when the calendar
month has increased and the calendar day and time is greater than or equal
to the start. Weeks, quarters, and years follow from that.

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

| Type                        | Example literal                            |
|-----------------------------|--------------------------------------------|
| `INTERVAL YEAR`             | `INTERVAL '20' YEAR`                       |
| `INTERVAL YEAR TO MONTH`    | `INTERVAL '20-07' YEAR TO MONTH`           |
| `INTERVAL MONTH`            | `INTERVAL '10' MONTH`                      |
| `INTERVAL DAY`              | `INTERVAL '10' DAY`                        |
| `INTERVAL DAY TO HOUR`      | `INTERVAL '10 10' DAY TO HOUR`             |
| `INTERVAL DAY TO MINUTE`    | `INTERVAL '10 10:30' DAY TO MINUTE`        |
| `INTERVAL DAY TO SECOND`    | `INTERVAL '10 10:30:40.999' DAY TO SECOND` |
| `INTERVAL HOUR`             | `INTERVAL '12' HOUR`                       |
| `INTERVAL HOUR TO MINUTE`   | `INTERVAL '12:10' HOUR TO MINUTE`          |
| `INTERVAL HOUR TO SECOND`   | `INTERVAL '12:10:59' HOUR TO SECOND`       |
| `INTERVAL MINUTE`           | `INTERVAL '10' MINUTE`                     |
| `INTERVAL MINUTE TO SECOND` | `INTERVAL '80:01.001' MINUTE TO SECOND`    |
| `INTERVAL SECOND`           | `INTERVAL '80.001' SECOND`                 |

A leading negative sign applies to all fields; for example the
negative sign in the interval literal `INTERVAL '-1 2:03:04' DAYS TO
SECONDS` applies to both the days and hour/minute/second parts.

To specify an interval value with more than 2 digits you must specify
an increased precision for the corresponding type, e.g.:
`INTERVAL '100' HOUR(3)`

## Other date/time/timestamp/time interval operations

The following arithmetic operations are supported:

| Operation                   | Result Type        | Explanation                                                      |
|-----------------------------|--------------------|------------------------------------------------------------------|
| `DATE` + `INTERVAL`         | `DATE`             | Add an interval to a date                                        |
| `INTERVAL` + `INTERVAL`     | `INTERVAL`         | Add two intervals; both must have the same type                  |
| `TIMESTAMP` + `INTERVAL`    | `TIMESTAMP`        | Add an interval to a timestamp                                   |
| `TIME` + `INTERVAL`         | `TIME`             | Add an interval to a time. Performs wrapping addition.           |
| `-` `INTERVAL`              | `INTERVAL`         | Negate an interval                                               |
| `DATE` - `INTERVAL`         | `DATE`             | Subtract an interval from a date                                 |
| `TIME` - `TIME`             | `INTERVAL` (short) | Compute the difference between two times                         |
| `TIME` - `INTERVAL`         | `TIME`             | Subtract an interval from a time. Performs wrapping subtraction. |
| `TIMESTAMP` - `INTERVAL`    | `TIMESTAMP`        | Subtract an interval from a timestamp                            |
| `INTERVAL` - `INTERVAL`     | `INTERVAL`         | Subtract two intervals                                           |
| `INTERVAL` * `DOUBLE`       | `INTERVAL`         | Multiply an interval by a scalar                                 |
| `INTERVAL` / `DOUBLE`       | `INTERVAL`         | Divide an interval by a scalar                                   |
| `TIMESTAMP` - `TIMESTAMP`   | `INTERVAL` (long)  | Subtract two timestamps, convert result into days                |

Arithmetic involving a `TIME` value always produces a (positive)
`TIME` value, between `00:00:00` (inclusive) and `24:00:00`
(exclusive).  One can think of the computation as being performed in
nanoseconds, and then performing a modulo operation with the number of
nanoseconds in a day.  For this reason, adding or subtracting a long
interval from a `TIME` value is supported, but always leaves the
`TIME` value unchanged (since long intervals always consist of a whole
number of days).

Arithmetic between a DATE and an INTERVAL first converts the interval
to a whole number days (rounding down) and then performs the
computation on whole days.

## Timezones

`DATE`, `TIME` and `TIMESTAMP` have no time zone.

## Important unsupported operations

Since DBSP is a *deterministic* query engine, it does not currently
offer support for any function that depends on the current time.  So
the following are *not* supported: `LOCALTIME`, `LOCALTIMESTAMP`,
`CURRENT_TIME`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`.

## Date formatting

We support the following functions for formatting date-like values:

| Operation     | Arguments           | Example                              |
|---------------|---------------------|--------------------------------------|
| `FORMAT_DATE` | string_format, date | `FORMAT_DATE("%Y=%m", d)` => 2020-10 |

These functions are similar to the BigQuery functions:
<https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_elements_date_time>

The format string recognizes the following format specifiers:
(the Types column encodes the following types: D=Date, TS=TIMESTAMP, T=TIME

| Element | Types | Description                                                                                                                                                                                                                                                                                     | Example                  |
|---------|-------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|
| %A      | D,TS  | The full weekday name (English)                                                                                                                                                                                                                                                                 | Wednesday                |
| %a      | D,TS  | The abbreviated weekday name (English)                                                                                                                                                                                                                                                          | Wed                      |
| %B      | D,TS  | The full month name (English)                                                                                                                                                                                                                                                                   | January                  |
| %b      | D,TS  | The abbreviated month name (English)                                                                                                                                                                                                                                                            | Jan                      |
| %C      | D,TS  | The century (a year divided by 100 and truncated to an integer) as a decimal number (00-99)                                                                                                                                                                                                     | 20                       |
| %c      | TS    | The date and time representation (English)                                                                                                                                                                                                                                                      | Wed Jan 20 21:47:00 2021 |
| %D      | D, TS | The date in the format %m/%d/%y                                                                                                                                                                                                                                                                 | 01/20/21                 |
| %d      | D, TS | The day of the month as a decimal number (01-31)                                                                                                                                                                                                                                                | 20                       |
| %e      | D, TS | The day of the month as a decimal number (1-31); single digits are preceded by a space                                                                                                                                                                                                          | 20                       |
| %F      | D, TS | The date in the format %Y-%m-%d                                                                                                                                                                                                                                                                 | 2021-01-20               |
| %G      | D, TS | The ISO 8601 year with century as a decimal number. Each ISO year begins on the Monday before the first Thursday of the Gregorian calendar year. Note that %G and %Y may produce different results near Gregorian year boundaries, where the Gregorian year and ISO year can diverge            | 2021                     |
| %g      | D, TS | The ISO 8601 year without century as a decimal number (00-99). Each ISO year begins on the Monday before the first Thursday of the Gregorian calendar year. Note that %g and %y may produce different results near Gregorian year boundaries, where the Gregorian year and ISO year can diverge | 21                       |
| %H      | TS, T | The hour (24-hour clock) as a decimal number (00-23)                                                                                                                                                                                                                                            | 21                       |
| %h      | D, TS | Same as %b                                                                                                                                                                                                                                                                                      | Jan                      |
| %I      | TS, T | The hour (12-hour clock) as a decimal number (01-12)                                                                                                                                                                                                                                            | 09                       |
| %j      | D, TS | The day of the year as a decimal number (001-366)                                                                                                                                                                                                                                               | 020                      |
| %k      | TS, T | The hour (24-hour clock) as a decimal number (0-23); single digits are preceded by a space.                                                                                                                                                                                                     | 21                       |
| %l      | TS, T | The hour (12-hour clock) as a decimal number (1-12); single digits are preceded by a space.                                                                                                                                                                                                     | 9                        |
| %M      | TS, T | The minute as a decimal number (00-59)                                                                                                                                                                                                                                                          | 47                       |
| %m      | D, TS | The month as a decimal number (01-12)                                                                                                                                                                                                                                                           | 01                       |
| %P      | TS, T | When formatting, this is either am or pm. This cannot be used with parsing. Instead, use %p.                                                                                                                                                                                                    | pm                       |
| %p      | TS, T | When formatting, this is either AM or PM. When parsing, this can be used with am, pm, AM, or PM.                                                                                                                                                                                                | PM                       |
| %R      | TS, T | The time in the format %H:%M                                                                                                                                                                                                                                                                    | 21:47                    |
| %S      | TS, T | The second as a decimal number (00-60)                                                                                                                                                                                                                                                          | 00                       |
| %s      | TS, T | The number of seconds since 1970-01-01 00:00:00.                                                                                                                                                                                                                                                | 1611179220               |
| %T      | TS, T | The time in the format %H:%M:%S                                                                                                                                                                                                                                                                 | 21:47:00                 |
| %U      | D, TS | The week number of the year (Sunday as the first day of the week) as a decimal number (00-53)                                                                                                                                                                                                   | 03                       |
| %u      | D, TS | The weekday (Monday as the first day of the week) as a decimal number (1-7)                                                                                                                                                                                                                     | 3                        |
| %V      | D, TS | The ISO 8601 week number of the year (Monday as the first day of the week) as a decimal number (01-53). If the week containing January 1 has four or more days in the new year, then it is week 1; otherwise it is week 53 of the previous year, and the next week is week 1                    | 03                       |
| %W      | D, TS | The week number of the year (Monday as the first day of the week) as a decimal number (00-53)                                                                                                                                                                                                   | 03                       |
| %w      | D, TS | The weekday (Sunday as the first day of the week) as a decimal number (0-6)                                                                                                                                                                                                                     | 3                        |
| %X      | TS, T | The time representation in HH:MM:SS format                                                                                                                                                                                                                                                      | 21:47:00                 |
| %x      | D, TS | The date representation in MM/DD/YY format                                                                                                                                                                                                                                                      | 01/20/21                 |
| %Y      | D, TS | The year with century as a decimal number                                                                                                                                                                                                                                                       | 2021                     |
| %y      | D, TS | The year without century as a decimal number (00-99), with an optional leading zero.                                                                                                                                                                                                            | 21                       |
| %Z      | TS    | The time zone name                                                                                                                                                                                                                                                                              | UTC-5                    |
| %z      | TS    | The offset from the Prime Meridian in the format +HHMM or -HHMM as appropriate, with positive values representing locations east of Greenwich                                                                                                                                                   | -0500                    |
| %n      |       | A newline character.                                                                                                                                                                                                                                                                            |                          |
| %t      |       | A tab character                                                                                                                                                                                                                                                                                 |                          |
| %%      |       | A single % character                                                                                                                                                                                                                                                                            | %                        |

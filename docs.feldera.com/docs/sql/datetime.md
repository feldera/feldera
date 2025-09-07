# Date- and Time-Related Operations

## Time units

The following are legal time units:

| Time unit         | Meaning                                                                                                                                                                                                                                                                                                    |
|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <a id="millenium"></a>`MILLENNIUM`      | A thousand years                                                                                                                                                                                                                                                                                           |
| <a id="century"></a>`CENTURY`         | A hundred years; a number between 1 and 10                                                                                                                                                                                                                                                                 |
| <a id="decade"></a>`DECADE`          | Ten years; a number between 1 and 10                                                                                                                                                                                                                                                                       |
| <a id="year"></a>`YEAR`            | One year; can be positive or negative                                                                                                                                                                                                                                                                      |
| <a id="quarter"></a>`QUARTER`         | 1/4 of a year; a number between 1 and 4                                                                                                                                                                                                                                                                    |
| <a id="month"></a>`MONTH`           | One month; a number between 1 and 12                                                                                                                                                                                                                                                                       |
| <a id="week"></a>`WEEK`            | Seven days. The definition of "week" is quite involved: The year's first week is the week containing the first Thursday of the year or either the week containing the 4th of January or either the week that begins between 29th of Dec. and 4th of Jan. The week number is thus a value between 0 and 53. |
| <a id="doy"></a>`DOY`             | Day of year, a number between 1 and 366                                                                                                                                                                                                                                                                    |
| <a id="dow"></a>`DOW`             | Day of week, with Sunday being 1 and Saturday being 7                                                                                                                                                                                                                                                      |
| <a id="isodow"></a>`ISODOW`          | ISO day of the week, with Monday 1 and Sunday 7                                                                                                                                                                                                                                                            |
| <a id="day"></a>`DAY`             | A day within a month, a number between 1 and 31                                                                                                                                                                                                                                                            |
| <a id="hour"></a>`HOUR`            | An hour within a day, a number between 0 and 23                                                                                                                                                                                                                                                            |
| <a id="minute"></a>`MINUTE`          | A minute within an hour, a number between 0 and 59                                                                                                                                                                                                                                                         |
| <a id="second"></a>`SECOND`          | A second within a minute, a number between 0 and 59                                                                                                                                                                                                                                                        |
| <a id="millisecond"></a>`MILLISECOND`     | A millisecond within a *minute*, including the number of seconds multiplied by 1000, a number between 0 and 59,999                                                                                                                                                                                         |
| <a id="microsecond"></a>`MICROSECOND`     | A microsecond within a *minute*, including the number of seconds multiplied by 1,000,000, a number between 0 and 59,999,999                                                                                                                                                                                |
| <a id="nanosecond"></a>`NANOSECOND`       | A nanosecond within a *minute*, including the number of seconds multiplied by 1,000,000,000, a number between 0 and 59,999,999,9999                                                                                                                                                                        |
| <a id="epoch"></a>`EPOCH`           | Number of seconds from Unix epoch, i.e., 1970/01/01.                                                                                                                                                                                                                                                       |
| <a id="sql_tsi_year"></a>`SQL_TSI_YEAR`    | Same as `YEAR`                                                                                                                                                                                                                                                                                             |
| <a id="sql_tsi_quarter"></a>`SQL_TSI_QUARTER` | Same as `QUARTER`                                                                                                                                                                                                                                                                                          |
| <a id="sql_tsi_month"></a>`SQL_TSI_MONTH`   | Same as `MONTH`                                                                                                                                                                                                                                                                                            |
| <a id="sql_tsi_week"></a>`SQL_TSI_WEEK`    | Same as `WEEK`                                                                                                                                                                                                                                                                                             |
| <a id="sql_tsi_hour"></a>`SQL_TSI_HOUR`    | Same as `HOUR`                                                                                                                                                                                                                                                                                             |
| <a id="sql_tsi_day"></a>`SQL_TSI_DAY`     | Same as `DAY`                                                                                                                                                                                                                                                                                              |
| <a id="sql_tsi_minute"></a>`SQL_TSI_MINUTE`  | Same as `MINUTE`                                                                                                                                                                                                                                                                                           |
| <a id="sql_tsi_second`"></a>`SQL_TSI_SECOND`  | Same as `SECOND`                                                                                                                                                                                                                                                                                           |

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

<a id="date_trunc"></a>
`DATE_TRUNC(date, <unit>)`, where `<unit>` is a time unit, as
described above, between `MILLENNIUM` and `DAY`.  Result is a `DATE`.
Rounds down the date to the specified time unit.  Example:
`DATE_TRUNC('2020-01-10', MONTH)` produces the result
`2020-01-01`.

<a id="date_extract"></a>
`EXTRACT(<unit> FROM date)` where `<unit>` is a time unit, as
described above.  Result is always a `BIGINT` value.
`DATE_PART` is a synonym for `EXTRACT`.

The following abbreviations can be used as well:

<a id="date_year"></a>
`YEAR(date)` is an abbreviation for `EXTRACT(YEAR FROM date)`.

<a id="date_month"></a>
`MONTH(date)` is an abbreviation for `EXTRACT(MONTH FROM date)`.

<a id="date_dayofmonth"></a>
`DAYOFMONTH(date)` is an abbreviation for `EXTRACT(DAY FROM
date)`.

<a id="date_dayofweek"></a>
`DAYOFWEEK(date)` is an abbreviation for `EXTRACT(DOW FROM
date)`.

<a id="date_hour"></a>
`HOUR(date)` is an abbreviation for `EXTRACT(HOUR FROM date)`.
For dates it always returns 0, since dates have no time component.

<a id="date_minute"></a>
`MINUTE(date)` is an abbreviation for `EXTRACT(MINUTE FROM date)`.
For dates it always returns 0, since dates have no time component.

<a id="date_second"></a>
`SECOND(date)` is an abbreviation for `EXTRACT(SECOND FROM date)`.
For dates it always returns 0, since dates have no time component.

<a id="date_floor"></a>
`FLOOR(date TO <unit>)`, where `<unit>` is a time unit.

<a id="date_ceil"></a>
`CEIL(date TO <unit>)`, where `<unit>` is a time unit.

Values of type `DATE` can be compared using `=`, `<>`, `!=`, `<`, `>`,
`<=`, `>=`, `<=>`, `BETWEEN`; the result is a Boolean.

<a id="date_timestampdiff"></a>
`TIMESTAMPDIFF(<unit>, left, right)` computes the difference (right - left) between
two dates values and expresses the result in the specified time units.
The result is a 32-bit integer.

## Times

A time represents the time of day, a value between 0 and 24 hours
(excluding the latter).  Time values are stored to a precision of
nanoseconds -- 9 subsecond digits.  Leap seconds are not supported.

### Time literals

`TIME` literals have the form `TIME 'HH:MM:SS.FFF'`, where the
fractional part is optional, and can have between 0 and 9 digits.  An
example is: '23:59:59.132'.  The hours must be between 0 and 23, the
minutes between 0 and 59, and the seconds between 0 and 59.  Exactly
two digits must be used for hours, minutes, and seconds.  Spaces are
not allowed between quotes.

### Time operations

<a id="time_trunc"></a>
`TIME_TRUNC(time, <unit>)`, where `<unit>` is a time unit,
as described above, between `HOUR` and `SECOND`.  Result is a
`TIME`.  Rounds down the time to the specified time unit.
Example: `TIME_TRUNC('12:34:56.78', MINUTE)` produces the
result `12:34:00`.

<a id="time_extract"></a>
`EXTRACT(<unit> FROM time)` where `<unit>` is a time unit from
`HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`; the semantics is as
described above.  Result is always a `BIGINT` value.

The following abbreviations can be used as well:

<a id="time_hour"></a>
`HOUR(time)` is an abbreviation for `EXTRACT(HOUR FROM time)`.

<a id="time_minute"></a>
`MINUTE(time)` is an abbreviation for `EXTRACT(MINUTE FROM time)`.

<a id="time_second"></a>
`SECOND(time)` is an abbreviation for `EXTRACT(SECOND FROM
time)`.

Values of type `TIME` can be compared using `=`, `<>`, `!=`, `<`, `>`,
`<=`, `>=`, `<=>`, `BETWEEN`; the result is a Boolean.

<a id="timestamp_timestampdiff"></a>
`TIMESTAMPDIFF(<unit>, left, right)` computes the difference (right - left) between
two time values and expresses the result in the specified time units.
The result is a 32-bit integer.

<a id="time_floor"></a>
`FLOOR(time TO <unit>)`, where `<unit>` is a time unit between `HOUR` and
`MICROSECOND`.

<a id="time_ceil"></a> `CEIL(time TO <unit>)`, where `<unit>` is a
time unit between `HOUR` and `MICROSECOND`.

## Timestamps

The `TIMESTAMP` data type represents values composed of a `DATE` (as
described above) and a `TIME`.  `TIMESTAMP`s are represented with a
precision of milliseconds (3 digits for fractions of second).

### Timestamp literals

`TIMESTAMP` literals have the form `TIMESTAMP 'YYYY-MM-DD
HH:MM:SS.FFF'`, where the fractional part is optional.  Trailing
spaces are not allowed.

Timestamp literals can only represent 4-digit year positive values.
Values BC or values greater than 10,000 years are not supported.

The following operations are available on timestamps:

### Operations on timestamps

A cast from a numeric value to a `TIMESTAMP` interprets the numeric
value as an (big integer) number of milliseconds since the Unix epoch.
Conversely, a cast from a `TIMESTAMP` to a numeric value retrieves the
number of milliseconds since the Unix epoch from the timestamp.

<a id="timestamp_trunc"></a>
`TIMESTAMP_TRUNC(timestamp, <unit>)`, where `<unit>` is a time unit,
as described above, between `MILLENNIUM` and `SECOND`.  Result is a
`TIMESTAMP`.  Rounds down the timestamp to the specified time unit.
Example: `TIMESTAMP_TRUNC('2020-01-10 10:00:00', MONTH)` produces the
result `2020-01-01 00:00:00`.

<a id="timestamp_extract"></a>
`EXTRACT(<unit> FROM timestamp)` where `<unit>` is a time unit, as
described above.  Result is always a `BIGINT` value.

The following abbreviations can be used as well:

<a id="timestamp_year"></a>
`YEAR(timestamp)` is an abbreviation for `EXTRACT(YEAR FROM timestamp)`.

<a id="timestamp_month"></a>
`MONTH(timestamp)` is an abbreviation for `EXTRACT(MONTH FROM timestamp)`.

<a id="timestamp_dayofmonth"></a>
`DAYOFMONTH(timestamp)` is an abbreviation for `EXTRACT(DAY FROM
timestamp)`.

<a id="timestamp_dayofweek"></a>
`DAYOFWEEK(timestamp)` is an abbreviation for `EXTRACT(DOW FROM
timestamp)`.

<a id="timestamp_hour"></a>
`HOUR(timestamp)` is an abbreviation for `EXTRACT(HOUR FROM timestamp)`.

<a id="timestamp_minute"></a>
`MINUTE(timestamp)` is an abbreviation for `EXTRACT(MINUTE FROM timestamp)`.

<a id="timestamp_second"></a>
`SECOND(timestamp)` is an abbreviation for `EXTRACT(SECOND FROM
timestamp)`.

<a id="timestamp_floor"></a>
`FLOOR(timestamp TO <unit>)`, where `<unit>` is a time unit.

<a id="timestamp_ceil"></a>
`CEIL(timestamp TO <unit>)`, where `<unit>` is a time unit.

Values of type `TIMESTAMP` can be compared using `=`, `<>`, `!=`, `<`,
`>`, `<=`, `>=`, `<=>`, `BETWEEN`; the result is a Boolean.

<a id="timestamp_timestampdiff"></a>
`TIMESTAMPDIFF(<unit>, left, right)` computes the difference (right - left) between
two timestamps and expresses the result in the specified time units.
The result is a 32-bit integer.  `DATEDIFF` is a synonym for
`TIMESTAMPDIFF`.  One month is considered elapsed when the calendar
month has increased and the calendar day and time is greater than or equal
to the start. Weeks, quarters, and years follow from that.

<a id="timestampadd"></a>
`TIMESTAMPADD(<unit>, integer, timestamp)` adds an interval in the
specified unit to a timestamp or date/time literal. The added value can be negative.  The type of the
result is as follows:

- Adding anything to a `TIMESTAMP` value produces a `TIMESTAMP` result
- Adding an interval of hours, minutes, or seconds to a `DATE` produces a `TIMESTAMP` result
- Adding an interval of days, months, or longer to a `DATE` produces a `DATE` result
- Adding an interval of hours, minutes, or seconds to a `TIME` produces a `TIME` result

To create a timestamp using the Unix EPOCH in seconds as a base, you
can use the `TIMESTAMPADD` function.  The following code creates a
MAKE_TIMESTAMP function which creates a `TIMESTAMP` given a number of
seconds:

```sql
CREATE FUNCTION MAKE_TIMESTAMP(SECONDS BIGINT) RETURNS TIMESTAMP AS
TIMESTAMPADD(SECOND, SECONDS, DATE '1970-01-01');
```

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

`EXTRACT(unit FROM interval)` extracts the specified value from an `INTERVAL`.
Short intervals support EXTRACT of values between DAYS and MILLISECONDS.
Long intervals support EXTRACT of values between MILLENIUM and MONTHS.

## Other date/time/timestamp/time interval operations

The following arithmetic operations are supported:

| Operation                         | Result Type        | Explanation                                                      |
|-----------------------------------|--------------------|------------------------------------------------------------------|
| _date_ `+` _interval_             | `DATE`             | Add an interval to a date                                        |
| (_date_ `-` _date_) shortInterval | `INTERVAL`         | Compute the difference between two dates as a short interval     |
| (_date_ `-` _date_) longInterval  | `INTERVAL`         | Compute the difference between two dates as a long interval      |
| (_time_ `-` _time_) shortInterval | `INTERVAL`         | Compute the difference between two times as a short interval     |
| _interval_ `+` _interval_         | `INTERVAL`         | Add two intervals; both must have the same type                  |
| _timestamp_ `+` _interval_        | `TIMESTAMP`        | Add an interval to a timestamp                                   |
| _time_ `+` _interval_             | `TIME`             | Add an interval to a time. Performs wrapping addition.           |
| `-` _interval_                    | `INTERVAL`         | Negate an interval                                               |
| _date_ `-` _interval_             | `DATE`             | Subtract an interval from a date                                 |
| _time_ `-` _interval_             | `TIME`             | Subtract an interval from a time. Performs wrapping subtraction. |
| _timestamp_ `-` _interval_        | `TIMESTAMP`        | Subtract an interval from a timestamp                            |
| (_timestamp_ `-` _timestamp_) shortInterval | `INTERVAL` | Compute the difference between two timestamps as a short interval|
| (`TIMESTAMP` `-` `TIMESTAMP`) longInterval  | `INTERVAL` | Compute the difference between two timestamps as a long interval |
| _interval_ `-` _interval_         | `INTERVAL`         | Subtract two intervals                                           |
| _interval_ `*` _double_           | `INTERVAL`         | Multiply an interval by a scalar                                 |
| _interval_ `/` _double_           | `INTERVAL`         | Divide an interval by a scalar                                   |

Arithmetic involving a `TIME` value always produces a (positive)
`TIME` value, between `00:00:00` (inclusive) and `24:00:00`
(exclusive).  One can think of the computation as being performed in
nanoseconds, and then performing a modulo operation with the number of
nanoseconds in a day.  For this reason, adding or subtracting a long
interval from a `TIME` value is supported, but always leaves the
`TIME` value unchanged (since long intervals always consist of a whole
number of days).

Arithmetic between a `DATE` and an `INTERVAL` first converts the
interval to a whole number days (rounding down) and then performs the
computation on whole days.

<a id="date_sub"></a> `DATE_SUB` is a synonim for `DATE` - `INTERVAL`.
<a id="date_add"></a> `DATE_ADD` is a synonim for `DATE` + `INTERVAL`.
<a id="abs"></a>`ABS`(interval) computes the absolute value of an
interval.

## Timezones

`DATE`, `TIME` and `TIMESTAMP` have no time zone.

## `NOW`

The `NOW()` function returns the current date and time as a
`TIMESTAMP` value.  More precisely, it returns the date and time
when the current **step** of the pipeline was triggered.  A step
is triggered when the pipeline receives one or more new inputs or after a
user-configurable period of time if no new inputs arrive.
When executing a step, the pipeline incrementally updates all its views.
In particular, views that depend on the value of `NOW()` are updated
using the new current time.  The value of `NOW()` remains constant
within a step.  The value returned by the `NOW()` function
is a `TIMESTAMP WITHOUT TIMEZONE` returning the current time in the UTC
timezone, and thus is guaranteed to always be increasing.

By default, in the absence of new inputs, a step is triggered every
100 milliseconds.  This behavior is controlled by the
`clock_resolution_usecs` pipeline configuration setting.

| Operation     | Description         | Example                        |
|---------------|---------------------|--------------------------------|
| `NOW`         | Returns a timestamp | `NOW()` => 2024-07-10 00:00:00 |

:::warning

Programs that use `NOW()` can be very inefficient.  For example, a
program such as `SELECT T.x + NOW() FROM T` has to scan the
entire table T at every step.  Use this function judiciously.

:::

Note however that a specific class of `WHERE` and `HAVING` expressions
that use `NOW()` can be implemented very efficiently.  These are the
so-called "temporal filters".  Here is an example:

```sql
SELECT * FROM T WHERE T.ts >= NOW() - INTERVAL 1 DAYS;
```

In general, a temporal filter will involve inequality or equality
comparisons between an expression and a monotone function of the NOW
result.  A conjunction of such terms is also accepted if all terms
involve the same expression (e.g.: `T.ts >= NOW() - INTERVAL 1 DAYS
AND T.ts <= NOW() + INTERVAL 1 DAYS`).

## Date parsing and formatting

We support the following functions for formatting and parsing date-like values:

| Operation          | Arguments             | Result    | Example                              |
|--------------------|-----------------------|-----------|--------------------------------------|
| `FORMAT_DATE`      | string_format, date   | string    | `FORMAT_DATE('%Y-%m', DATE '2020-10-10')` => `2020-10` |
| `PARSE_DATE`       | string_format, string | DATE      | `PARSE_DATE(' %Y-%m-%d', '   2020-10-01')` => `2020-10-01` |
| `PARSE_TIME`       | string_format, string | TIME      | `PARSE_TIME('%H:%M', '10:10')` => `10:10:00` |
| `PARSE_TIMESTAMP`  | string_format, string | TIMESTAMP | `PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2020-10-01 00:00:00')` => `2020-10-01 00:00:00` |

If the string cannot be parsed according to the specified format:

- a runtime error is produced if the format is incorrect for producing
  a value of the required type
- the result is `NULL` if string cannot be parsed according to the format

For the parsing functions the format string must have enough elements
to obtain all elements required for building the result.  For example,
`PARSE_TIME('%I:%M:%S', '10:00:00')` will produce a runtime error,
since the `%I` element does not have enough information to infer
whether the hour is AM or PM (an extra `%p` element is needed).

The format string recognizes the following format specifiers; this
definition follows the Rust `strftime` spec:
https://docs.rs/chrono/latest/chrono/format/strftime/index.html

## Date Specifiers

|Spec. | Example | Description |
|------|---------|-------------|
| %Y   |2001     |The full proleptic Gregorian year, zero-padded to 4 digits. chrono supports years from -262144 to 262143. Note: years before 1 BCE or after 9999 CE, require an initial sign (+/-). |
| %C   |20       |The proleptic Gregorian year divided by 100, zero-padded to 2 digits. |
| %y   |01       |The proleptic Gregorian year modulo 100, zero-padded to 2 digits. |
| %m   |07       |Month number (01–12), zero-padded to 2 digits. |
| %b   |Jul      |Abbreviated month name. Always 3 letters. |
| %B   |July     |Full month name. Also accepts corresponding abbreviation in parsing. |
| %h   |Jul      |Same as %b. |
| %d   |08       |Day number (01–31), zero-padded to 2 digits. |
| %e   | 8       |Same as %d but space-padded. Same as %_d. |
| %a   |Sun      |Abbreviated weekday name. Always 3 letters. |
| %A   |Sunday   |Full weekday name. Also accepts corresponding abbreviation in parsing. |
| %w   |0        |Sunday = 0, Monday = 1, …, Saturday = 6. |
| %u   |7        |Monday = 1, Tuesday = 2, …, Sunday = 7. (ISO 8601) |
| %U   |28       |Week number starting with Sunday (00–53), zero-padded to 2 digits. |
| %W   |27       |Same as %U, but week 1 starts with the first Monday in that year instead. |
| %G   |2001     |Same as %Y but uses the year number in ISO 8601 week date. |
| %g   |01       |Same as %y but uses the year number in ISO 8601 week date. |
| %V   |27       |Same as %U but uses the week number in ISO 8601 week date (01–53). |
| %j   |189      |Day of the year (001–366), zero-padded to 3 digits. |
| %D   |07/08/01 |      Month-day-year format. Same as %m/%d/%y. |
| %F   |2001-07-08|     Year-month-day format (ISO 8601). Same as %Y-%m-%d. |
| %v   |8-Jul-2001|    Day-month-year format. Same as %e-%b-%Y. |

## Time Specifiers

|Spec. | Example | Description |
|------|---------|-------------|
| %H   |00       |Hour number (00–23), zero-padded to 2 digits. |
| %k   | 0       |Same as %H but space-padded. Same as %_H. |
| %I   |12       |Hour number in 12-hour clocks (01–12), zero-padded to 2 digits. |
| %l   |12       |Same as %I but space-padded. Same as %_I. |
| %P   |am       |am or pm in 12-hour clocks. |
| %p   |AM       |AM or PM in 12-hour clocks. |
| %M   |34       |Minute number (00–59), zero-padded to 2 digits. |
| %S   |60       |Second number (00–60), zero-padded to 2 digits. |
| %f   |26490000 |Number of nanoseconds since last whole second.  |
| %.f  |.026490  |Decimal fraction of a second. Consumes the leading dot. |
| %.3f |.026     |Decimal fraction of a second with a fixed length of 3. |
| %.6f |.026490  |Decimal fraction of a second with a fixed length of 6. |
| %.9f |.026490000|Decimal fraction of a second with a fixed length of 9. |
| %3f  |026      |Decimal fraction of a second like %.3f but without the leading dot. |
| %6f  |026490   |Decimal fraction of a second like %.6f but without the leading dot. |
| %9f  |026490000|Decimal fraction of a second like %.9f but without the leading dot. |
| %R   |00:34    |Hour-minute format. Same as %H:%M. |
| %T   |00:34:60 |Hour-minute-second format. Same as %H:%M:%S. |

## Time Zone Specifiers

These are currently unused

|Spec. | Example | Description |
|------|---------|-------------|
| %Z   |ACST     |Local time zone name. Skips all non-whitespace characters during parsing. Identical to %:z when formatting. |
| %z   |+0930    |Offset from the local time to UTC (with UTC being +0000). |
| %:z  |+09:30   |Same as %z but with a colon. |
| %::z |+09:30:00|Offset from the local time to UTC with seconds. |
| %:::z|+09      |Offset from the local time to UTC without minutes. |
| %#z  |+09      |Parsing only: Same as %z but allows minutes to be missing or present. |

## Timestamp Specifiers

|Spec. | Example | Description |
|------|---------|-------------|
| %+   |2001-07-08T00:34:60.026490+09:30 |      ISO 8601 / RFC 3339 date & time format. |
| %s   |994518299|      UNIX timestamp, the number of seconds since 1970-01-01 00:00 UTC. |

## Special Specifiers

|Spec. | Example | Description |
|------|---------|-------------|
| %t   |         |Literal tab (\t). |
| %n   |         |Literal newline (\n). |
| %%   |         |Literal percent sign. |

It is possible to override the default padding behavior of numeric specifiers %?. This is not allowed for other specifiers.
|Modifier |      Description |
|---------|------------------|
|%-?      |Suppresses any padding including spaces and zeroes. (e.g. %j = 012, %-j = 12) |
|%_?      |Uses spaces as a padding. (e.g. %j = 012, %_j =  12) |
|%0?      |Uses zeroes as a padding. (e.g. %e =  9, %0e = 09) |

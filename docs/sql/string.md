# String operations

SQL defines two primary character types: `character varying(n)` and
`character(n)`, where n is a positive integer.  Both of these types
can store strings up to n characters (not bytes) in length. An attempt
to store a longer string into a column of these types will result in
an error, unless the excess characters are all spaces, in which case
the string will be truncated to the maximum length. (This somewhat
bizarre exception is required by the SQL standard.)  If the string to
be stored is shorter than the declared length, values of type
character will be space-padded; values of type character varying will
simply store the shorter string.

In addition, we provide the `text`, or `varchar` type, which stores
strings of any length.

Trailing spaces are removed when converting a character value to one
of the other string types.  Note that trailing spaces are semantically
significant in character varying and text values, and when using
pattern matching (e.g., LIKE  and regular expressions).

## String constants (literals)

A string constant in SQL is an arbitrary sequence of characters
bounded by single quotes (`'`), for example `'This is a string'`. To
include a single-quote character within a string constant, write two
adjacent single quotes, e.g., `'Dianne''s horse'`. Note that this is
not the same as a double-quote character (`"`).

Two string constants that are only separated by whitespace with at
least one newline are concatenated and effectively treated as if the
string had been written as one constant. For example:

```sql
SELECT 'foo'
'bar'
```

is equivalent to:

```sql
SELECT 'foobar'
```

but:

```sql
SELECT 'foo'      'bar'
```

is not valid syntax.

## Escaped characters

We also accepts escaped characters withing string constants, which are
an extension to the SQL standard.  Within an escape string, a
backslash character (`\`) begins a C-like backslash escape sequence, in
which the combination of backslash and following character(s)
represent a special byte value:

|Backslash Escape Sequence|Interpretation|
|-------------------------|--------------|
|<code>\b</code>          | backspace    |
|<code>\f</code>          | form feed    |
|<code>\n</code>          | newline      |
|<code>\r</code>          | carriage return |
|<code>\t</code>          | tab          |
|<code>\o, \oo, \ooo</code> | (o = 0–7) octal byte value |
|<code>\xh, \xhh (h = 0–9, A–F)</code> | hexadecimal byte value |
|<code>\uxxxx, \Uxxxxxxxx (x = 0–9, A–F)</code> | 16 or 32-bit hexadecimal Unicode character value |

Any other character following a backslash is taken literally. Thus, to
include a backslash character, write two backslashes `\\`. Also, a
single quote can be included in an escape string by writing `\'`, in
addition to the normal way of `''`.

## Operations on string values

<table>
  <tr>
    <th>Operation</th>
    <th>Description</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td><code>||</code></td>
    <td>String concatenation (infix).  Note that concatenation does *not* strip trailing spaces
        from CHAR(N) values, unlike other SQL dialects.  If such behavior is desired, an explicit
        cast to `varchar` can be added.</td>
    <td><code>'Post' || 'greSQL'</code> => <code>PostgreSQL</code></td>
  </tr>
  <tr>
    <td><code>string LIKE pattern [ESCAPE escape-character]</code> and
        <code>string NOT LIKE pattern [ESCAPE escape-character]</code></td>
    <td>The LIKE expression returns true if the string matches the supplied pattern.
     (As expected, the <code>NOT LIKE</code> expression returns false if LIKE returns true.</td>
    <td>See below for details.</td>
  </tr>
  <tr>
    <td><code>string RLIKE pattern</code> and
        <code>string NOT RLIKE pattern</code></td>
    <td>The RLIKE expression returns true if the string matches the supplied pattern.
        The pattern is a standard Java regular expression.</td>
    <td><code>'string' RLIKE 's..i.*'</code> => <code>TRUE</code></td>
  </tr>
  <tr>
    <td><code>ASCII ( string )</code></td>
    <td>Returns the numeric code of the first character of the argument. In UTF8 encoding, returns the Unicode code point of the character. In other multibyte encodings, the argument must be an ASCII character.  Returns 0 if the string is empty.</td>
    <td><code>ascii('x')</code> => <code>120</code></td>
  </tr>
  <tr>
    <td><code>CHAR_LENGTH(string)</code> or <code>CHARACTER_LENGTH(string)</code></td>
    <td>Returns number of characters in the string.</td>
    <td><code>char_length('josé')</code> => <code>4</code></td>
  </tr>
  <tr>
    <td><code>CHR ( integer )</code></td>
    <td>Returns a string containing the character with the given code. If the code is incorrect (e.g., negative), the result is an empty string.</td>
    <td><code>chr(65)</code> => <code>A</code></td>
  </tr>
  <tr>
    <td><code>CONCAT(</code>string1, ..., stringN<code>)</code></td>
    <td>String concatenation.  Can have any number of arguments.</td>
    <td><code>CONCAT('Post', 'greSQL', 1)</code> => <code>PostgreSQL1</code></td>
  </tr>
  <tr>
    <td><code>INITCAP ( string )</code></td>
    <td>Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.</td>
    <td><code>initcap('hi THOMAS')</code> => <code>Hi Thomas</code></td>
  </tr>
  <tr>
    <td><code>LEFT ( string, count )</code></td>
    <td>Returns first <code>count</code> characters in the string.  If any argument is <code>NULL</code>, return <code>NULL</code>.</td>
    <td><code>left('abcde', 2)</code> => <code>ab</code></td>
  </tr>
  <tr>
    <td><code>LOWER ( string )</code></td>
    <td>Converts the string to all lower case.</td>
    <td><code>lower('TOM')</code> => <code>tom</code></td>
  </tr>
  <tr>
    <td><code>OVERLAY ( string PLACING newsubstring FROM start [ FOR remove ] )</code></td>
    <td>Replaces the substring of string that starts at the start'th character and extends for remove characters with newsubstring. If count is omitted, it defaults to the length of newsubstring.  If 'start' is nott positive, the original string is unchanged.  If 'start' is bigger than the length of 'string', the result is the concatenation of the two strings.  If 'remove' is negative it is considered 0.</td>
    <td><code>overlay('Txxxxas' placing 'hom' from 2 for 4)</code> => <code>Thomas</code></td>
  </tr>
  <tr>
    <td><code>POSITION(substring IN string)</code></td>
    <td>Returns first starting index of the specified substring within string, or zero if it's not present.  First character has index 1.</td>
    <td><code>position('om' in 'Thomas')</code> => <code>3</code></td>
  </tr>
  <tr>
    <td><code>REPEAT ( string, count )</code></td>
    <td>Repeats string the specified number of times.  The result is an empty string for a negative or 0 count.</td>
    <td><code>repeat('Pg', 4)</code> => <code>PgPgPgPg</code></td>
  </tr>
  <tr>
    <td><code>REPLACE ( haystack, needle, replacement )</code></td>
    <td>Replaces all occurrences of `needle` in `haystack` with `replacement`.</td>
    <td><code>replace('abcdefabcdef', 'cd', 'XX')</code> => <code>abXXefabXXef</code></td>
  </tr>
  <tr>
    <td><code>RLIKE(string, pattern)</code></td>
    <td>A function equivalent to the <code>RLIKE</code> operator above.</td>
    <td><code>RLIKE('string', 's..i.*')</code> => <code>TRUE</code></td>
  </tr>
  <tr>
    <td><code>SPLIT(string [, delimiter])</code></td>
    <td>Produce an array of strings, by splitting the first argument at each delimiter occurrence.
        If the delimiter is empty, return an array with the original string.  If the original
        string is empty, return an empty array.  If either argument is `NULL`, return `NULL`.
        If delimiter is absent assume it is the string <code>','</code>.</td>
    <td><code>SPLIT('a|b|c|', '|')</code> => { 'a', 'b', 'c', '' }</td>
  </tr>
  <tr>
    <td><code>SUBSTRING (</code> string <code>[ FROM</code> start <code>] [ FOR</code> count<code> ] )</code></td>
    <td>Extracts the substring of string starting at the "start"'th character if that is specified, and stopping after "count" characters if the value is specified. At least one of "start" or "count" must be provided.  If "start" is negative, it is replaced with 1.  If "count" is negative the empty string is returned.  The index of the first character is 1.</td>
    <td><code>SUBSTRING('Thomas' from 2 for 3)</code> => <code>hom</code><br>
        <code>SUBSTRING('Thomas' from 3)</code> => <code>omas</code><br>
        <code>SUBSTRING('Thomas' for 2)</code> => <code>Th</code></td>
  </tr>
  <tr>
    <td><code>TRIM ( [ LEADING | TRAILING | BOTH ]</code> characters <code>FROM</code> string <code>)</code></td>
    <td>Remove the specified characters from the specified ends of the string argument</td>
    <td><code>TRIM(both 'xyz' from 'yxTomxx')</code> => <code>Tom</code><br>
        <code>TRIM(leading 'xyz' from 'yxTomxx')</code> => <code>Tomxx</code>
    </td>
  </tr>
  <tr>
    <td><code>UPPER ( string )</code></td>
    <td>Converts the string to all upper case.</td>
    <td><code>upper('tom')</code> => <code>TOM</code></td>
  </tr>
</table>

<!--
  <tr>
    <td><code>REGEXP_REPLACE(expr, pat, repl[, pos[, occurrence[, match_type]]])</code></td>
    <td>Replaces occurrences in the string expr that match the regular expression
        specified by the pattern pat with the replacement string repl, and returns
        the resulting string. If `expr`, `pat`, or `repl` is `NULL`, the return value is `NULL`.</td>
  </tr>
-->

## `LIKE`

string `LIKE` pattern [`ESCAPE` escape-character]

string `NOT LIKE` pattern [`ESCAPE` escape-character]

If pattern does not contain percent signs or underscores, then the
pattern only represents the string itself; in that case `LIKE` acts
like the equals operator. An underscore (`_`) in pattern stands for
(matches) any single character; a percent sign (`%`) matches any
sequence of zero or more characters.

Some examples:

```sql
'abc' LIKE 'abc'    true
'abc' LIKE 'a%'     true
'abc' LIKE '_b_'    true
'abc' LIKE 'c'      false
```

`LIKE` pattern matching always covers the entire string. Therefore, if
it's desired to match a sequence anywhere within a string, the pattern
must start and end with a percent sign.

To match a literal underscore or percent sign without matching other
characters, the respective character in pattern must be preceded by
the escape character. The default escape character is the backslash
but a different one can be selected by using the ESCAPE clause. To
match the escape character itself, write two escape characters.  The
escape character cannot be one of the special pattern characters `_`
or `%`.

Some examples where the escape character is changed to `#`:

```sql
SELECT 'hawkeye' LIKE 'h%' ESCAPE '#'          true
SELECT 'hawkeye' NOT LIKE 'h%' ESCAPE '#'      false
SELECT 'h%' LIKE 'h#%' ESCAPE '#'              true
SELECT 'h%' NOT LIKE 'h#%' ESCAPE '#'          false
SELECT 'h%wkeye' LIKE 'h#%' ESCAPE '#'         false
SELECT 'h%wkeye' NOT LIKE 'h#%' ESCAPE '#'     true
SELECT 'h%wkeye' LIKE 'h#%%' ESCAPE '#'        true
SELECT 'h%wkeye' NOT LIKE 'h#%%' ESCAPE '#'    false
SELECT 'h%awkeye' LIKE 'h#%a%k%e' ESCAPE '#'   true
```

When either argument or `LIKE`, `NOT LIKE` is `NULL`, the result is `NULL`.

## POSIX regular expressions

Regular expressions are matched using the `RLIKE` function.  If either
argument of `RLIKE` is `NULL`, the result is also `NULL`.

The description below is from the [Postgres
documentation](https://www.postgresql.org/docs/15/functions-matching.html#FUNCTIONS-POSIX-REGEXP),
where credit is given to Henry Spencer.

POSIX regular expressions provide a more powerful means for pattern
matching than the `LIKE` and `SIMILAR TO` operators.  Many Unix tools
such as `egrep`, `sed`, or `awk` use a pattern matching language that
is similar to the one described here.

Currently our compiler does *not* support `SIMILAR TO` regular
expressions.

A regular expression is a character sequence that is an abbreviated
definition of a set of strings (a regular set). A string is said to
match a regular expression if it is a member of the regular set
described by the regular expression. As with `LIKE`, pattern
characters match string characters exactly unless they are special
characters in the regular expression language — but regular
expressions use different special characters than `LIKE` does. Unlike
`LIKE` patterns, a regular expression is allowed to match anywhere
within a string, unless the regular expression is explicitly anchored
to the beginning or end of the string.

A *regular expression* is defined as one or more *branches*, separated
by `|`. It matches anything that matches one of the branches.

A *branch* is zero or more *quantified atoms* or *constraints*,
concatenated. It matches a match for the first, followed by a match
for the second, etc.; an empty branch matches the empty string.

A *quantified atom* is an *atom* possibly followed by a single
*quantifier*. Without a quantifier, it matches a match for the
atom. With a quantifier, it can match some number of matches of the
atom. An atom can be any of the possibilities shown in the Table
below.

<table>
   <tr>
      <th>Atom</th>
      <th>Description</th>
   </tr>
   <tr>
      <td><code>(re)</code></td>
      <td>where <code>re</code> is any regular expression: matches a match for <code>re</code>, with the match noted for possible reporting</td>
   </tr>
   <tr>
      <td><code>(?:re)</code></td>
      <td>as above, but the match is not noted for reporting (a “non-capturing” set of parentheses)</td>
   </tr>
   <tr>
      <td><code>.</code></td>
      <td>matches any single character</td>
   </tr>
   <tr>
      <td><code>[chars]</code></td>
      <td>a bracket expression, matching any one of the chars (see below for more details)</td>
   </tr>
   <tr>
      <td><code>\k</code></td>
      <td>where <code>k</code> is a non-alphanumeric character): matches that character taken as an ordinary character, e.g.,
          <code>\\</code> matches a backslash character</td>
   </tr>
   <tr>
      <td><code>\c</code></td>
      <td>where <code>c</code> is alphanumeric (possibly followed by other characters): is an escape, see below</td>
   </tr>
   <tr>
      <td><code>&#123;</code></td>
      <td>when followed by a character other than a digit, matches the left-brace character <code>&#123;</code>;
          when followed by a digit, it is the beginning of a bound (see below)</td>
   </tr>
   <tr>
      <td>x</td>
      <td>where <code>x</code> is a single character with no other significance, matches that character</td>
   </tr>
</table>

The possible quantifiers and their meanings are shown the Table below.

|Quantifier|Matches|
|----------|-------|
|`*`  | a sequence of 0 or more matches of the atom|
|`+`    | a sequence of 1 or more matches of the atom|
|`?`    | a sequence of 0 or 1 matches of the atom|
|`{`m`}`  | a sequence of exactly m matches of the atom|
|`{`m`,}` | a sequence of m or more matches of the atom|
|`{`m`,`n`}`| a sequence of m through n (inclusive) matches of the atom; m cannot exceed n|

<!--
|`*?`   | non-greedy version of *|
|`+?`   | non-greedy version of +|
|`??`   | non-greedy version of ?|
|`{`m`}?` | non-greedy version of {m}|
|`{`m`,}?`| non-greedy version of {m,}|
|`{`m`,`n`}?` | non-greedy version of {m,n}|
-->

A constraint matches an empty string, but matches only when specific
conditions are met. A constraint can be used where an atom could be
used, except it cannot be followed by a quantifier. The simple
constraints are shown in the Table below; some more constraints are
described later.

|Constraint|    Description|
|----------|---------------|
|`^`       |    matches at the beginning of the string |
|`$`       |    matches at the end of the string       |

<!--
|(?=re)    |    positive lookahead matches at any point where a substring matching re begins (AREs only)
|(?!re)    |    negative lookahead matches at any point where no substring matching re begins (AREs only)
|(?<=re)   |    positive lookbehind matches at any point where a substring matching re ends (AREs only)
|(?<!re)   |    negative lookbehind matches at any point where no substring matching re ends (AREs only)
-->


### Bracket Expressions

A bracket expression is a list of characters enclosed in `[]`. It
normally matches any single character from the list (but see
below). If the list begins with `^`, it matches any single character
not from the rest of the list. If two characters in the list are
separated by `-`, this is shorthand for the full range of characters
between those two (inclusive) in the collating sequence, e.g., `[0-9]`
in ASCII matches any decimal digit. It is illegal for two ranges to
share an endpoint, e.g., `a-c-e`.  Ranges are very
collating-sequence-dependent, so portable programs should avoid
relying on them.

To include a literal `]` in the list, make it the first character
(after `^`, if that is used). To include a literal `-`, make it the
first or last character, or the second endpoint of a range. To use a
literal `-` as the first endpoint of a range, enclose it in `[.` and
`.]`.  With the exception of these characters, some combinations using
`[`, all other special characters lose their special significance
within a bracket expression. In particular, `\` is not special.

Within a bracket expression, the name of a character class enclosed in
`[:` and `:]` stands for the list of all characters belonging to that
class. A character class cannot be used as an endpoint of a range. The
POSIX standard defines these character class names:

|Class| Description                          |
|-----|--------------------------------------|
|`alnum`| letters and numeric digits           |           
|`alpha`| letters                              |
|`blank`| space and tab                        |
|`cntrl`| control characters                   |
|`digit`| numeric digits                       |
|`graph`| printable characters except space    |
|`lower`| lower-case letters                   |
|`print`| printable characters including space |
|`punct`| punctuation                          |
|`space`| any white space                      |
|`upper`| upper-case letters                   |
|`xdigit`| hexadecimal digits                   |

Class-shorthand escapes provide shorthands for certain commonly-used
character classes. They are shown in the table below.

|Escape|        Description|
|------|-------------------|
|`\d`    |matches any digit, like [[:digit:]] |
|`\s`    |matches any whitespace character, like [[:space:]] |
|`\w`    |matches any word character, like [[:word:]] |
|`\D`    |matches any non-digit, like [^[:digit:]] |
|`\S`    |matches any non-whitespace character, like [^[:space:]] |
|`\W`    |matches any non-word character, like [^[:word:]] |

The behavior of these standard character classes is generally
consistent across platforms for characters in the 7-bit ASCII set.
Whether a given non-ASCII character is considered to belong to one of
these classes depends on the collation that is used for the
regular-expression function or operator.

### Regular Expression Escapes

Escapes are special sequences beginning with `\` followed by an
alphanumeric character.  Escapes come in several varieties: character
entry, class shorthands, constraint escapes, and back references. A
`\` followed by an alphanumeric character but not constituting a valid
escape is illegal.

Character-entry escapes exist to make it easier to specify
non-printing and other inconvenient characters in REs. They are shown
in the Table below.

|Escape|        Description|
|------|-------------------|
|`\a`     |alert (bell) character, as in C|
|`\b`     |backspace, as in C |
|`\B`     |synonym for backslash (\) to help reduce the need for backslash doubling|
|`\c`X    |(where X is any character) the character whose low-order 5 bits are the same as those of X, and whose other bits are all zero|
|`\e`     |the character whose collating-sequence name is ESC, or failing that, the character with octal value 033 |
|`\f`     |form feed, as in C |
|`\n`     |newline, as in C |
|`\r`     |carriage return, as in C |
|`\t`     |horizontal tab, as in C |
|`\u`wxyz |(where wxyz is exactly four hexadecimal digits) the character whose hexadecimal value is 0xwxyz |
|`\v`     |vertical tab, as in C|
|`\x`hhh  |(where hhh is any sequence of hexadecimal digits) the character whose hexadecimal value is 0xhhh (a single character no matter how many hexadecimal digits are used)|
|`\0`     |the character whose value is 0 (the null byte)|
|`\`xy    |(where xy is exactly two octal digits, and is not a back reference) the character whose octal value is 0xy|
|`\`xyz   |(where xyz is exactly three octal digits, and is not a back reference) the character whose octal value is 0xyz|

Hexadecimal digits are 0-9, a-f, and A-F. Octal digits are 0-7.

A constraint escape is a constraint, matching the empty string if
specific conditions are met, written as an escape. They are shown in
the Table below.

|Escape|Description|
|------|-----------|
|`\A`     |matches only at the beginning of the string (see Section 9.7.3.5 for how this differs from ^)|
|`\m`     |matches only at the beginning of a word|
|`\M`     |matches only at the end of a word|
|`\y`     |matches only at the beginning or end of a word|
|`\Y`     |matches only at a point that is not the beginning or end of a word|
|`\Z`     |matches only at the end of the string|

A back reference `(\`n`)` matches the same string matched by the
previous parenthesized subexpression specified by the number n (see
the Table below). For example, `([bc])\1` matches `bb` or `cc` but not
`bc` or `cb`. The subexpression must entirely precede the back
reference in the RE. Subexpressions are numbered in the order of their
leading parentheses. Non-capturing parentheses do not define
subexpressions. The back reference considers only the string
characters matched by the referenced subexpression, not any
constraints contained in it. For example, `(^\d)\1` will match `22`.

|Escape|        Description|
|------|-------------------|
|`\`m     |(where m is a nonzero digit) a back reference to the m'th subexpression|
|`\`mnn   |(where m is a nonzero digit, and nn is some more digits, and the decimal value mnn is not greater than the number of closing capturing parentheses seen so far) a back reference to the mnn'th subexpression|

<!--
### Regular expression functions

```sql
REGEXP_REPLACE(expr, pat, repl[, pos[, occurrence[, match_type]]])
```

Replaces occurrences in the string `expr` that match the regular
expression specified by the pattern `pat` with the replacement string
`repl`, and returns the resulting string. If any of `expr`, `pat`, or
`repl` is `NULL`, the return value is `NULL`.

`REGEXP_REPLACE()` takes these optional arguments:

- `pos`: The position in expr at which to start the search. If omitted, the default is 1.

- `occurrence`: Which occurrence of a match to replace. If omitted, the default is 0 (which
  means “replace all occurrences”).

- `match_type`: A string that specifies how to perform matching.  It is a string that may
  contain any or all the following characters specifying how to perform matching:

  - `c`: Case-sensitive matching.

  - `i`: Case-insensitive matching.

  - `m`: Multiple-line mode. Recognize line terminators within the string. The default
     behavior is to match line terminators only at the start and end of the string expression.

  - `n`: The `.` character matches line terminators. The default is for `.` matching to stop at the end of a line.

  - `u`: Unix-only line endings. Only the newline character is recognized as a line ending by the ., ^, and $ match operators.
-->
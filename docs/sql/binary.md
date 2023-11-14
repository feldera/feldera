# Binary (byte array) operations

The `BINARY` and `VARBINARY` data types allows storage of binary strings.

A binary string is a sequence of octets (or bytes). Binary strings are
distinguished from character strings in two ways. First, binary
strings specifically allow storing octets of value zero and other
“non-printable” octets (usually, octets outside the decimal range 32
to 126).  Character strings disallow zero octets, and also disallow
any other octet values and sequences of octet values that are invalid
according to the database's selected character set encoding.  Second,
operations on binary strings process the actual bytes, whereas the
processing of character strings depends on locale settings. In short,
binary strings are appropriate for storing data that the programmer
thinks of as “raw bytes”, whereas character strings are appropriate
for storing text.

## Binary literals

`BINARY` and `VARBINARY` literals are specified with by string
literals of hexadecimal digits with an `x` prefix: `x'45F0AB'`.  Such
a literal must have an even number of characters, and all characters
must be legal hexadecimal digits.  A multi-part literal can be
specified as the concatenation of multiple literals, e.g.: `x'AB' 'CD'`.

## Binary value operations

Binary values support bitwise operations, although there is no syntax
yet to express these operations.  They can be aggregated using the
aggregation functions `BIT_AND`, `BIT_OR`, and `BIT_XOR`.

<table>
  <tr>
    <th>Operation</th>
    <th>Description</th>
    <th>Examples</th>
  </tr>
  <tr>
    <td><code>||</code></td>
    <td>Concatenation of binary values</td>
    <td><code>x'ab' || x'cd'</code> => <code>x'ABCD'</code></td>
  </tr>
  <tr>
    <td><code>TO_HEX</code>(binary)</td>
    <td>Generate a `VARCHAR` string describing the value in hexadecimal</td>
    <td><code>TO_HEX(x'0abc')</code> => <code>'0ABC'</code></td>
  </tr>
</table>

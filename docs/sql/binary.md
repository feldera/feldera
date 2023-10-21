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

## Binary string operations


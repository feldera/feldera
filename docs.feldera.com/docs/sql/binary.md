# Binary (Byte Array) Operations

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

`BINARY` and `VARBINARY` literals are specified by string literals of
hexadecimal digits with an `x` prefix: `x'45F0AB'`.  Such a literal
must have an even number of characters, and all characters must be
legal hexadecimal digits.  A multi-part literal can be specified as
the concatenation of multiple literals, e.g.: `x'AB' 'CD'`.

Converting a string to a binary value produces a binary value
containing the bytes of the encoding of the characters of the string.

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
    <td><a id="bin2utf8"></a><code>BIN2UTF8</code></td>
    <td>Convert a BINARY value into a VARCHAR value by reinterpreting the content as UTF-8 characters; returns NULL on failure</td>
    <td><code>BIN2UTF8(x'4041')</code> => <code>'@A'</code></td>
  </tr>
  <tr>
    <td><a id="concat"></a><code>||</code></td>
    <td>Concatenation of binary values</td>
    <td><code>x'ab' || x'cd'</code> => <code>x'ABCD'</code></td>
  </tr>
  <tr>
    <td><a id="gunzip"></a><code>GUNZIP</code>(binary)</td>
    <td>
        Decompresses a binary string using the GZIP algorithm.
        If the input data is not in the gzip format this function fails at runtime.
        The output is the decompressed data as a `VARCHAR` string.
        If the input is NULL, NULL is returned.
    </td>
    <td><code>SELECT gunzip(x'1f8b08000000000000ff4b4bcd49492d4a0400218115ac07000000')</code> => <code>feldera</code></td>
  </tr>
  <tr>
    <td><a id="left"></a><code>LEFT ( binary, count )</code></td>
    <td>Returns first <code>count</code> bytes in the byte array.  If any argument is <code>NULL</code>, return <code>NULL</code>.</td>
    <td><code>left(x'abcdef', 2)</code> => <code>abcd</code></td>
  </tr>
  <tr>
    <td><a id="md5"></a><code>MD5</code>(binary)</td>
    <td>
        Calculates an MD5 128-bit checksum of the argument and returns it as a hex <code>VARCHAR</code> value.
        If the input is NULL, NULL is returned.
    </td>
    <td><code>SELECT md5(x'0123456789ABCDEF')</code> => <code>a1cd1d1fc6491068d91007283ed84489</code></td>
  </tr>
  <tr>
    <td><a id="octet_length"></a><code>OCTET_LENGTH</code>(binary)</td>
    <td>Returns the number of bytes in the binary</td>
    <td><code>OCTET_LENGTH(x'0abc')</code> => <code>2</code></td>
  </tr>
  <tr>
    <td><a id="overlay"></a><code>OVERLAY</code>(binary1 PLACING binary2 FROM integer1 [ FOR integer2 ])</td>
    <td>
        Generate a binary string that replaces substring of binary1 with binary2.
        The substring of binary1 starts at the byte specified by integer1 and extends for integer2 bytes.
        If integer1 is greater than the byte length of binary1, concatenation is performed.
        If integer2 is smaller than the byte length of binary2, the substring still gets replaced by the entirety of binary2, producing a binary string with greater byte length than binary1
    </td>
    <td><code>SELECT overlay(x'1234567890'::bytea placing x'0203' from 2 for 3)</code> => <code>x'12020390'</code></td>
  </tr>
  <tr>
    <td><a id="position"></a><code>POSITION</code>(binary1 IN binary2)</td>
    <td>Returns the position of the first occurrence of binary1 in binary2. The first offset is 1. If binary1 isn't found in binary2, 0 is returned</td>
    <td><code>POSITION(x'20' IN x'102023')</code> => <code>2</code></td>
  </tr>
  <tr>
    <td><a id="right"></a><code>RIGHT ( binary, count )</code></td>
    <td>Returns last <code>count</code> bytes in the byte array.  If any argument is <code>NULL</code>, return <code>NULL</code>.</td>
    <td><code>right(x'abcdef', 2)</code> => <code>cdef</code></td>
  </tr>
  <tr>
    <td><a id="substring"></a><code>SUBSTRING</code>(binary FROM integer)</td>
    <td>Generate a substring of binary starting at the given offset in bytes. The first offset is 1. If the start position integer is less than 1, it is treated as 1</td>
    <td><code>SUBSTRING(x'123456', 3)</code> => <code>x'56'</code></td>
  </tr>
  <tr>
    <td><code>SUBSTRING</code>(binary FROM integer1 FOR integer2)</td>
    <td>Generate a substring of binary starting at the given offset in bytes with the given length. The first offset is 1. If the start position integer is less than 1, it is treated as 1. Negative lengths or offsets larger than length return an empty result.</td>
    <td><code>SUBSTRING(x'1234567890' FROM 3 FOR 2)</code> => <code>x'5678'</code></td>
  </tr>
  <tr>
    <td><a id="to_hex"></a><code>TO_HEX</code>(binary)</td>
    <td>Generate a `VARCHAR` string describing the value in hexadecimal</td>
    <td><code>TO_HEX(x'0abc')</code> => <code>'0ABC'</code></td>
  </tr>
  <tr>
    <td><a id="to_int"></a><a id="to_int"></a><code>TO_INT</code>(binary)</td>
    <td>Generate an `INT` value from the first 4 bytes of the `binary`, where the 0-th byte is the MSB</td>
    <td><code>TO_INT(x'0abc')</code> => <code>2748</code></td>
  </tr>
</table>

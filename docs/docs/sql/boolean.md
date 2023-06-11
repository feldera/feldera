# Boolean Operations

There are two Boolean literals: `TRUE` and `FALSE`.

We support the following Boolean operations:

## OR

The following truth table defines the `OR` operator:

<table>
  <tr>
    <th></th>
    <th>TRUE</th>
    <th>FALSE</th>
    <th>NULL</th>
  </tr>
  <tr>
    <th>TRUE</th>
    <td>TRUE</td>
    <td>TRUE</td>
    <td>TRUE</td>
  </tr>
  <tr>
    <th>FALSE</th>
    <td>TRUE</td>
    <td>FALSE</td>
    <td>NULL</td>
  </tr>
  <tr>
    <th>NULL</th>
    <td>TRUE</td>
    <td>NULL</td>
    <td>NULL</td>
  </tr>
</table>

## AND

The following truth table defines the `AND` operator:

<table>
  <tr>
    <th></th>
    <th>TRUE</th>
    <th>FALSE</th>
    <th>NULL</th>
  </tr>
  <tr>
    <th>TRUE</th>
    <td>TRUE</td>
    <td>FALSE</td>
    <td>NULL</td>
  </tr>
  <tr>
    <th>FALSE</th>
    <td>FALSE</td>
    <td>FALSE</td>
    <td>FALSE</td>
  </tr>
  <tr>
    <th>NULL</th>
    <td>NULL</td>
    <td>FALSE</td>
    <td>NULL</td>
  </tr>
</table>

## NOT

The following table defines the `NOT` operator:

<table>
  <tr>
    <th>TRUE</th>
    <td>FALSE</td>
  </tr>
  <tr>
    <th>FALSE</th>
    <td>TRUE</td>
  </tr>
  <tr>
    <th>NULL</th>
    <td>NULL</td>
  </tr>
</table>

## IS FALSE

The following table defines the `IS FALSE` operator:

<table>
  <tr>
    <th>TRUE</th>
    <td>FALSE</td>
  </tr>
  <tr>
    <th>FALSE</th>
    <td>TRUE</td>
  </tr>
  <tr>
    <th>NULL</th>
    <td>FALSE</td>
  </tr>
</table>

## IS NOT FALSE

The following table defines the `IS NOT FALSE` operator:

<table>
  <tr>
    <th>TRUE</th>
    <td>FALSE</td>
  </tr>
  <tr>
    <th>FALSE</th>
    <td>TRUE</td>
  </tr>
  <tr>
    <th>NULL</th>
    <td>TRUE</td>
  </tr>
</table>

## IS TRUE

The following table defines the `IS TRUE` operator:

<table>
  <tr>
    <th>TRUE</th>
    <td>TRUE</td>
  </tr>
  <tr>
    <th>FALSE</th>
    <td>FALSE</td>
  </tr>
  <tr>
    <th>NULL</th>
    <td>FALSE</td>
  </tr>
</table>

## IS NOT TRUE

The following table defines the `IS NOT TRUE` operator:

<table>
  <tr>
    <th>TRUE</th>
    <td>FALSE</td>
  </tr>
  <tr>
    <th>FALSE</th>
    <td>TRUE</td>
  </tr>
  <tr>
    <th>NULL</th>
    <td>TRUE</td>
  </tr>
</table>


:::info

Notice that not all Boolean operations produce `NULL` results when an operand is
`NULL`.
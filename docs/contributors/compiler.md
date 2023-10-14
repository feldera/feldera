# Using the SQL Compiler directly

## SQL programs

The compiler, which runs internally in the Feldera Platform, must be given the
table definition first, and then the view definition. e.g.,

```sql
-- define Person table
CREATE TABLE Person
(
    name    VARCHAR,
    age     INT,
    present BOOLEAN
);
CREATE VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18;
```

The compiler generates a Rust function which implements the query as a function:
given the input data, it produces the output data.

The compiler can also generate a function which will incrementally maintain the
view `Adult` when presented with changes to table `Person`:

```
                                           table changes
                                                V
tables -----> SQL-to-DBSP compiler ------> DBSP circuit
views                                           V
                                           view changes
```

## Command-line options

The compiler is invoked using a Linux shell script, called
`sql-to-dbsp`, residing in the directory `SQL-compiler` directory.
Here is an example:

```sh
$ ./sql-to-dbsp -h
Usage: sql-to-dbsp [options] Input file to compile
  Options:
    -h, --help, -?
      Show this message and exit
    --alltables
      Generate an input for each CREATE TABLE, even if the table is not used
      by any view
      Default: false
    --lenient
      Lenient SQL validation.  If true it allows duplicate column names in a
      view
      Default: false
    --outputsAreSets
      Ensure that outputs never contain duplicates
      Default: false
    --ignoreOrder
      Ignore ORDER BY clauses at the end
      Default: false
    -O
      Optimization level (0, 1, or 2)
      Default: 2
    -T
      Specify logging level for a class (can be repeated)
      Syntax: -Tkey=value
      Default: {}
    -d
      SQL syntax dialect used
      Default: ORACLE
      Possible Values: [BIG_QUERY, ORACLE, MYSQL, MYSQL_ANSI, SQL_SERVER, JAVA]
    -f
      Name of function to generate
      Default: circuit
    -i
      Generate an incremental circuit
      Default: false
    -j
      Emit JSON instead of Rust
      Default: false
    -je
      Emit error messages as a JSON array to stderr
      Default: false
    -jpg, -png
      Emit a JPG or PNG image of the circuit instead of Rust
      Default: false
    -js
      Emit a JSON file containing the schema of all views and tables involved
    -o
      Output file; stdout if null
    -q
      Quiet: do not print warnings
      Default: false
```

Here is a description of several command-line options:

- O: sets the optimization level.  Note that some programs may not
     compile at optimization level 0, since that level inhibits all
     front-end (Calcite) optimizations, and some Calcite optimizations
     are required to eliminate constructs that are not supported by
     the back-end.

--lenient: Some SQL queries generate output views having multiple columns
     with the same name.  Such views can cause problems with other tools
     that interface with the compiler outputs.  By default the compiler will
     emit an error when given such views.  For example, the following definition:

     `CREATE VIEW V AS SELECT T.COL2, S.COL2 from T, S`

     will create a view with two columns named `COL2`.  The workaround is to
     explicitly name the view columns, e.g.:

     `CREATE VIEW V AS SELECT T.COL2 AS TCOL2, S.COL2 AS SCOL2 from T, S`

     Using the `--lenient` flag will only emit warnings, but compile such programs.

--ignoreOrder: `ORDER BY` clauses are not naturally incrementalizable.  Using this
     flag directs the compiler to ignore `ORDER BY` clauses that occur *last*
     in a view definition.  This will not affect `ORDER BY` clauses in an `OVER`
     clause, or `ORDER BY` clauses followed by `LIMIT` clauses.  The use of this flag
     is recommended with the `-i` flag that incrementalizes the compiler output.

--outputsAreSets: SQL queries can produce outputs that contain duplicates, but
     such outputs are rarely useful in practice.  Using this flag will ensure that
     each output VIEW does not contain duplicates.  This can also be ensured by
     using a `SELECT DISTINCT` statement in the view definition.  An example query
     that can produce duplicates is:

     `CREATE VIEW V AS SELECT T.COL1 FROM T`

-d: Sets the lexical rules used.  SQL dialects differ in rules for
     allowed identifiers, quoting identifiers, conversions to
     uppercase, case sensitivity of identifiers.

## Compiling a SQL program to Rust

The following command-line compiles a script called `x.sql` and writes
the result in a file `lib.rs`:

```sh
$ ./sql-to-dbsp x.sql -o ../temp/src/lib.rs
```

Let's assume we are compiling a file
containing the program in the example above.

In the generated program every `CREATE TABLE` is translated to an
input, and every `CREATE VIEW` is translated to an output.  The result
produced will look like this:

```rust
$ cat ../temp/src/lib.rs
// Automatically-generated file

[...boring stuff removed...]
fn circuit() -> impl FnMut(OrdZSet<Tuple3<Option<String>, Option<i32>, Option<bool>>, Weight>) -> (OrdZSet<Tuple1<Option<String>>, Weight>, ) {
    let PERSON = Rc::new(RefCell::<OrdZSet<Tuple3<Option<String>, Option<i32>, Option<bool>>, Weight>>::new(Default::default()));
    let PERSON_external = PERSON.clone();
    let PERSON = Generator::new(move || PERSON.borrow().clone());
    let ADULT = Rc::new(RefCell::<OrdZSet<Tuple1<Option<String>>, Weight>>::new(Default::default()));
    let ADULT_external = ADULT.clone();
    let root = dbsp::RootCircuit::build(|circuit| {
        // CREATE TABLE `PERSON` (`NAME` VARCHAR, `AGE` INTEGER, `PRESENT` BOOLEAN)
        // DBSPSourceOperator 64
        // CREATE TABLE `PERSON` (`NAME` VARCHAR, `AGE` INTEGER, `PRESENT` BOOLEAN)
        let PERSON = circuit.add_source(PERSON);
        // rel#46:LogicalProject.(input=LogicalTableScan#1,inputs=0..1)
        // DBSPMapOperator 83
        let stream0: Stream<_, OrdZSet<Tuple2<Option<String>, Option<i32>>, Weight>> = PERSON.map(move |t: &Tuple3<Option<String>, Option<i32>, Option<bool>>, | ->
        Tuple2<Option<String>, Option<i32>> {
            Tuple2::new(t.0.clone(), t.1)
        });
        // rel#48:LogicalFilter.(input=LogicalProject#46,condition=>($1, 18))
        // DBSPFilterOperator 103
        let stream1: Stream<_, OrdZSet<Tuple2<Option<String>, Option<i32>>, Weight>> = stream0.filter(move |t: &Tuple2<Option<String>, Option<i32>>, | ->
        bool {
            wrap_bool(gt_i32N_i32(t.1, 18i32))
        });
        // rel#50:LogicalProject.(input=LogicalFilter#48,inputs=0)
        // DBSPMapOperator 119
        let stream2: Stream<_, OrdZSet<Tuple1<Option<String>>, Weight>> = stream1.map(move |t: &Tuple2<Option<String>, Option<i32>>, | ->
        Tuple1<Option<String>> {
            Tuple1::new(t.0.clone())
        });
        // CREATE VIEW `ADULT` AS
        // SELECT `PERSON`.`NAME`
        // FROM `schema`.`PERSON` AS `PERSON`
        // WHERE `PERSON`.`AGE` > 18
        // DBSPSinkOperator 121
        stream2.inspect(move |m| { *ADULT.borrow_mut() = m.clone() });
        Ok(())}).unwrap();
    return move |PERSON| {
        *PERSON_external.borrow_mut() = PERSON;
        root.0.step().unwrap();
        return (ADULT_external.borrow().clone(), );
    };
}
```

You can compile the generated Rust code:

```sh
$ cd ../temp
$ cargo build
```

The generated file contains a Rust function called `circuit` (you can
change its name using the compiler option `-f`).  Calling `circuit`
will return an executable DBSP circuit handle, and a DBSP catalog.
These APIs can be used to execute the circuit.

## Obtaining the schema information from the compiler

The `-js` compiler flag is followed by a file name.  If the flag is
supplied, the compiler will write information about the input tables
and output views in JSON in the supplied file name.  Here is an
example of the generated JSON for the following program:

```sql
CREATE TABLE T (
  COL1 INT NOT NULL
, COL2 DOUBLE NOT NULL
, COL3 VARCHAR(3) PRIMARY KEY
, COL4 VARCHAR(3) ARRAY
)

CREATE VIEW V AS SELECT COL1 AS xCol FROM T
CREATE VIEW V1 (yCol) AS SELECT COL1 FROM T
```sql

Output:

```json
{
  "inputs" : [ {
    "name" : "T",
    "fields" : [ {
      "name" : "COL1",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "INTEGER",
        "nullable" : false
      }
    }, {
      "name" : "COL2",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "DOUBLE",
        "nullable" : false
      }
    }, {
      "name" : "COL3",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "VARCHAR",
        "nullable" : true,
        "precision" : 3
      }
    }, {
      "name" : "COL4",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "ARRAY",
        "nullable" : true,
        "component" : {
          "type" : "VARCHAR",
          "nullable" : false,
          "precision" : 3
        }
      }
    } ],
    "primary_key" : [ "COL3" ]
  } ],
  "outputs" : [ {
    "name" : "V",
    "fields" : [ {
      "name" : "xCol",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "INTEGER",
        "nullable" : false
      }
    } ]
  }, {
    "name" : "V1",
    "fields" : [ {
      "name" : "yCol",
      "case_sensitive" : true,
      "columntype" : {
        "type" : "INTEGER",
        "nullable" : false
      }
    } ]
  } ]
}
```

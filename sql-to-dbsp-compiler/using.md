# Using the SQL Compiler directly

## SQL programs

A SQL program consists of the table definitions, followed by view
definitions. e.g.:

```sql
-- define Person table
CREATE TABLE Person
(
    name    VARCHAR NOT NULL,
    age     INT,
    present BOOLEAN
);
CREATE VIEW Adult AS SELECT Person.name FROM Person WHERE Person.age > 18;
```

Given this program as input, the SQL compiler generates a Rust
function which implements the query as a function: given the input
data, it produces the output data.

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
    --alltables
      Generate an input for each CREATE TABLE, even if the table is not used
      by any view
      Default: false
    --dataflow
      Emit the Dataflow graph of the program in the specified JSON file
    --enterprise
      Generate code supporting enterprise features
      Default: false
    --errors
      Error output file; stderr if not specified
      Default: <empty string>
    --crates
      Followed by a program name. Generates code using multiple crates;
      `outputFile` is interpreted as a directory.
      Default: <empty string>
    --handles
      Use handles (true) or Catalog (false) in the emitted Rust code
      Default: false
    -h, --help, -?
      Show this message and exit
    --ignoreOrder
      Ignore ORDER BY clauses at the end
      Default: false
    --jdbcSource
      Connection string to a database that contains table metadata
      Default: <empty string>
    --je, -je
      Emit error messages as a JSON array to the error output
      Default: false
    --jpg, -jpg
      Emit a jpg image of the circuit instead of Rust
      Default: false
    --js, -js
      Emit a JSON file containing the schema of all views and tables in thes
      specified file.
    --lenient
      Lenient SQL validation.  If true it allows duplicate column names in a
      view
      Default: false
    --no-restrict-io
      Do not restrict the types of columns allowed in tables and views
      Default: false
    --noRust
      Do not generate Rust output files
      Default: false
    --nowstream
      Implement NOW as a stream (true) or as an internal operator (false)
      Default: false
    --outputsAreSets
      Ensure that outputs never contain duplicates
      Default: false
    --plan
      Emit the Calcite plan of the program in the specified JSON file
    --png, -png
      Emit a png image of the circuit instead of Rust
      Default: false
    --streaming
      Compiling a streaming program, where only inserts are allowed
      Default: false
    --trimInputs
      Do not ingest unused fields of input tables
      Default: false
    -O
      Optimization level (0, 1, or 2)
      Default: 2
    -i
      Generate an incremental circuit
      Default: false
    -o
      Output file; stdout if not specified
      Default: <empty string>
    -q
      Quiet: do not print warnings
      Default: false
    -v
      Output verbosity
      Default: 0
```

Here is a description of the non-obvious command-line options:

--handles: The Rust generated code can expose the input tables and
     output views in two ways: through explicit handles, and through a
     `Catalog` object.  The catalog allows one to retrieve the handles
     by name, but offers only *untyped* handles, that require a
     serializer (for output handles) or a deserializer (for input
     handles) to transmit data.  The handles API gives access to typed
     stream handles, which allow data insertion and retrieval without
     using serialization/deserialization.

--ignoreOrder: `ORDER BY` clauses are not naturally incrementalizable.
     Using this flag directs the compiler to ignore `ORDER BY` clauses
     that occur *last* in a view definition (thus giving unsorte
     outputs).  This will not affect `ORDER BY` clauses in an `OVER`
     clause, or `ORDER BY` clauses followed by `LIMIT` clauses.  The
     use of this flag is recommended with the `-i` flag that
     incrementalizes the compiler output.

--lenient: Some SQL queries generate output views having multiple columns
     with the same name.  Such views can cause problems with other tools
     that interface with the compiler outputs.  By default the compiler will
     emit an error when given such views.  For example, the following definition:

     `CREATE VIEW V AS SELECT T.COL2, S.COL2 from T, S`

     will create a view with two columns named `COL2`.  The workaround is to
     explicitly name the view columns, e.g.:

     `CREATE VIEW V AS SELECT T.COL2 AS TCOL2, S.COL2 AS SCOL2 from T, S`

     Using the `--lenient` flag will only emit warnings, but compile such programs.

--plan: Generates a JSON structure for each view compiled, representing the Calcite
     optimized plan for computing the view

--outputsAreSets: SQL queries can produce outputs that contain duplicates, but
     such outputs are rarely useful in practice.  Using this flag will ensure that
     each output VIEW does not contain duplicates.  This can also be ensured by
     using a `SELECT DISTINCT` statement in the view definition.  An example query
     that can produce duplicates is:

     `CREATE VIEW V AS SELECT T.COL1 FROM T`

-O:  sets the optimization level.  A higher values implies more optimizations.

-d:  Sets the lexical rules used.  SQL dialects differ in rules for
     allowed identifiers, quoting identifiers, conversions to
     uppercase, case sensitivity of identifiers.

--streaming: Equivalent to adding the following property to all program tables:
     `'appendOnly' = 'true'`.

--nowstream: If this property is set to 'true' it implements the NOW() function
     in a special way, as an input table, which allows deterministic testing.

     When a program uses the `NOW` function, the following input table is
     automatically injected by the compiler:

     ```sql
     CREATE TABLE NOW(now TIMESTAMP NOT NULL LATENESS INTERVAL 0 SECONDS);
     ```

     All invocations of the `NOW()` function within the program
     will produce the last value inserted in this table.

     This table does is not populated automatically.  Instead, the
     user is responsible for supplying the data to this table.  In
     every step of the circuit the user has to insert a new value in
     this table, which should be larger than the previous value.

### Example: Compiling a SQL program to Rust

The following command-line compiles a script called `x.sql` and writes
the result in a file `lib.rs`:

```sh
$ ./sql-to-dbsp x.sql --handles -o ../temp/src/lib.rs
```

Let's assume we are compiling a file
containing the program in the example above.

In the generated program every `CREATE TABLE` is translated to an
input, and every `CREATE VIEW` is translated to an output.  The result
produced will look like this^[1]:

[1] Note: the compiler output changes as the compiler implementation
    evolves.  This code is shown for illustrative purposes only.

```rust
$ cat ../temp/src/lib.rs
// Automatically-generated file

[...boring stuff removed...]
fn circuit(workers: usize) -> Result<(DBSPHandle, (CollectionHandle<Tup3<String, Option<i32>, Option<bool>>, Weight>, OutputHandle<OrdZSet<Tup1<String>, Weight>>, )), DbspError> {

    let (circuit, streams) = Runtime::init_circuit(workers, |circuit| {
        // CREATE TABLE `PERSON` (`NAME` VARCHAR NOT NULL, `AGE` INTEGER, `PRESENT` BOOLEAN)
        #[derive(Clone, Debug, Eq, PartialEq)]
        struct r#PERSON_0 {
            r#field: String,
            r#field_0: Option<i32>,
            r#field_1: Option<bool>,
        }
        impl From<PERSON_0> for Tup3<String, Option<i32>, Option<bool>> {
            fn from(table: r#PERSON_0) -> Self {
                Tup3::new(table.r#field,table.r#field_0,table.r#field_1,)
            }
        }
        impl From<Tup3<String, Option<i32>, Option<bool>>> for r#PERSON_0 {
            fn from(tuple: Tup3<String, Option<i32>, Option<bool>>) -> Self {
                Self {
                    r#field: tuple.0,
                    r#field_0: tuple.1,
                    r#field_1: tuple.2,
                }
            }
        }
        deserialize_table_record!(PERSON_0["PERSON", 3] {
            (r#field, "NAME", false, String, None),
            (r#field_0, "AGE", false, Option<i32>, Some(None)),
            (r#field_1, "PRESENT", false, Option<bool>, Some(None))
        });
        serialize_table_record!(PERSON_0[3]{
            r#field["NAME"]: String,
            r#field_0["AGE"]: Option<i32>,
            r#field_1["PRESENT"]: Option<bool>
        });
        // DBSPSourceMultisetOperator 312(32)
        // CREATE TABLE `PERSON` (`NAME` VARCHAR NOT NULL, `AGE` INTEGER, `PRESENT` BOOLEAN)
        let (PERSON, handlePERSON) = circuit.add_input_zset::<Tup3<String, Option<i32>, Option<bool>>, Weight>();

        // rel#36:LogicalFilter.(input=LogicalTableScan#1,condition=>($1, 18))
        // DBSPFilterOperator 332(57)
        let stream3: Stream<_, OrdZSet<Tup3<String, Option<i32>, Option<bool>>, Weight>> = PERSON.filter(move |t: &Tup3<String, Option<i32>, Option<bool>>, | ->
        bool {
            wrap_bool(gt_i32N_i32((*t).1, 18i32))
        });
        // rel#38:LogicalProject.(input=LogicalFilter#36,inputs=0)
        // DBSPMapOperator 350(79)
        let stream4: Stream<_, OrdZSet<Tup1<String>, Weight>> = stream3.map(move |t: &Tup3<String, Option<i32>, Option<bool>>, | ->
        Tup1<String> {
            Tup1::new((*t).0.clone())
        });
        // CREATE VIEW `ADULT` AS
        // SELECT `PERSON`.`NAME`
        // FROM `schema`.`PERSON` AS `PERSON`
        // WHERE `PERSON`.`AGE` > 18
        #[derive(Clone, Debug, Eq, PartialEq)]
        struct r#ADULT_0 {
            r#field: String,
        }
        impl From<ADULT_0> for Tup1<String> {
            fn from(table: r#ADULT_0) -> Self {
                Tup1::new(table.r#field,)
            }
        }
        impl From<Tup1<String>> for r#ADULT_0 {
            fn from(tuple: Tup1<String>) -> Self {
                Self {
                    r#field: tuple.0,
                }
            }
        }
        deserialize_table_record!(ADULT_0["ADULT", 1] {
            (r#field, "NAME", false, String, None)
        });
        serialize_table_record!(ADULT_0[1]{
            r#field["NAME"]: String
        });
        let handleADULT = stream4.output();

        Ok((handlePERSON, handleADULT, ))
    })?;
    Ok((circuit, streams))
}
```

You can compile the generated Rust code:

```sh
$ cd ../temp
$ cargo build
```

The generated file contains a Rust function called `circuit` (you can
change its name using the compiler option `-f`).  Calling `circuit`
will return an executable DBSP circuit handle, and a tuple containing
a handle for each input and output stream, in the order they are
declared in the SQL program.

#### Executing the produced circuit

We can write a unit test to exercise this circuit:

```rust
#[test]
pub fn test() {
    let (mut circuit, (person, adult) ) = circuit(2).unwrap();
    // Feed two input records to the circuit.
    // First input has a count of "1"
    person.push( (SqlString::from_ref("Bob"), Some(12), Some(true)).into(), 1 );
    // Second input has a count of "2"
    person.push( (SqlString::from_ref("Tom"), Some(20), Some(false)).into(), 2 );
    // Execute the circuit on these inputs
    circuit.step().unwrap();
    // Read the produced output
    let out = adult.consolidate();
    // Print the produced output
    println!("{}", out);
}
```

The unit test can be exercised with `cargo test -- --nocapture`.

This will print the output as:

```
layer:
    ("Tom",) -> 2
```

### Serialization and Deserialization

In general a circuit will connect with external data sources, and thus
will need to convert the data to/from other representations.  Here is
an example using the `Catalog` circuit API.  We compile the same
program as before, with different command-line flags:

```sh
$ ./sql-to-dbsp x.sql -i -o ../temp/src/lib.rs
```

This time we are producing an incremental version of the circuit.  The
difference between this circuit and the previous one is as follows:

- For the non-incremental circuit, every time we supply an input
  value, the table `PERSONS` is cleared and filled with the supplied
  value.  The `circuit.step()` function computes the contents of the
  output view `ADULTS`.  Reading the output handle gives us the entire
  contents of this view.

- For the incremental circuit, the `PERSONS` table is initially empty.
  Every time we supply an input it is *added* to the table.  (Input
  encoding formats such as [`JSON`](../formats/json) can also specify
  *deletions* from the table.)  The `circuit.step()` function computes
  the *changes* to the output view `ADULTS`.  Reading the output
  handle gives us the latest *changes* to the contents of this view.
  Formats such as CSV cannot represent deletions, so they may be
  insufficient for representing changes.  In the following example we
  input only insertions using CSV, but the output is received in a
  JSON format which can describe deletions too.

We exercise this circuit by inserting data using a CSV format:

```rust
#[test]
pub fn test() {
    use dbsp_adapters::{CircuitCatalog, RecordFormat};

    let (mut circuit, catalog) = circuit(2)
        .expect("Failed to build circuit");
    let persons = catalog
        .input_collection_handle(&SqlIdentifier::from("PERSON"))
        .expect("Failed to get input collection handle");
    let mut persons_stream = persons
        .configure_deserializer(RecordFormat::Csv(Default::default())
        .expect("Failed to configure deserializer");
    persons_stream
        .insert(b"Bob,12,true")
        .expect("Failed to insert data");
    persons_stream
        .insert(b"Tom,20,false")
        .expect("Failed to insert data");
    persons_stream
        .insert(b"Tom,20,false")
        .expect("Failed to insert data");  // Insert twice
    persons_stream.flush();
    // Execute the circuit on these inputs
    circuit
        .step()
        .unwrap();

    let adult = &catalog
        .output_handles(&SqlIdentifier::from("ADULT"))
        .expect("Failed to get output collection handles")
        .delta_handle;

    // Read the produced output
    let out = adult.consolidate();
    // Print the produced output
    println!("{:?}", out);
}
```

## Obtaining the schema information from the compiler

The `-js` compiler flag is followed by a file name.  If the flag is
supplied, the compiler will write information about the input tables
and output views in JSON in the supplied file name.  Here is an
example of the generated JSON for the following program:

```sql
CREATE TABLE T (
  COL1 INT NOT NULL
, COL2 DOUBLE NOT NULL FOREIGN KEY REFERENCES S(COL0)
, COL3 VARCHAR(3) NOT NULL PRIMARY KEY
, COL4 VARCHAR(3) ARRAY
)

CREATE VIEW V AS SELECT COL1 AS xCol FROM T
CREATE VIEW V1 (yCol) AS SELECT COL1 FROM T
```sql

Output:

```json
{
  "inputs" : [ {
    "name" : "t",
    "fields" : [ {
      "name" : "col1",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "INTEGER",
        "nullable" : false
      }
    }, {
      "name" : "col2",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "DOUBLE",
        "nullable" : false
      }
    }, {
      "name" : "col3",
      "case_sensitive" : false,
      "columntype" : {
        "type" : "VARCHAR",
        "nullable" : false,
        "precision" : 3
      }
    }, {
      "name" : "col4",
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
    "primary_key" : [ "col3" ],
    "materialized" : false,
    "foreign_keys" : [ {
      "columns" : [ "col2" ],
      "refers" : "s",
      "tocolumns" : [ "col0" ]
    } ]
  } ],
  "outputs" : [ {
    "name" : "v",
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

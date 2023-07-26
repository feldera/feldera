# Writing Feldera programs using SQL

## High-level program structure

Feldera only supports two kinds of SQL statements: table definition
statements, and view definition statements.  Each table definition
becomes an input, and each view definition becomes an output.
Here is an example program:

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

Statements need to be separated by semicolons.

## Incremental view maintenance

The Feldera runtime is optimized for performing incremental view
maintenance.  In consequence, Feldera programs in SQL are expressed as
VIEWS, or *standing queries*.  A view is essentially a computation
that describes a relation that is computed from other tables or views.

For example, the following query defines a view:

```sql
CREATE VIEW Adulg AS SELECT Person.name FROM Person WHERE Person.age > 18
```

In order to interpret this query the compiler needs to have been given
a definition of table (or view) Person.  The table `Person` must be
defined using a SQL Data Definition Language (DDL) statement, e.g.:

```SQL
CREATE TABLE Person
(
    name    VARCHAR,
    age     INT,
    present BOOLEAN
)
```

The compiler must be given the table definition first, and then the
view definition.  The compiler generates a Rust function which
implements the query as a function: given the input data, it produces
the output data.



The compiler can also generate a function which will incrementally
maintain the view `Adult` when presented with changes to table
`Person`:

```
                                           table changes
                                                V
tables -----> SQL-to-DBSP compiler ------> DBSP circuit
views                                           V
                                           view changes
```

## Command-line compiler

The compiler is invoked using a Linux shell script, called
`sql-to-dbsp`, residing in the directory `SQL-compiler` directory.
Here is an example:

```
$ ./sql-to-dbsp -h
Usage: sql-to-dbsp [options] Input file to compile
  Options:
    -h, --help, -?
      Show this message and exit
    -O
      Optimization level (0, 1, or 2)
      Default: 2
    -T
      Specify logging level for a class (can be repeated)
      Syntax: -Tkey=value
      Default: {}
    -alltables
      Generate an input for each CREATE TABLE, even if the table is not used
      by any view
      Default: false
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
$ ./sql-to-dbsp x.sql -o ../temp/src/lib.rs
```

The last command-line compiles a script called `x.sql` and writes the
result in a file `lib.rs`.  Let's assume we are compiling a file
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


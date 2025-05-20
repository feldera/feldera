package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Helper class for testing.  Holds together
 * - the compiler that is used to compile a program,
 * - the circuit, and
 * - the input/output data that is used to test the circuit.
 * Submits the code for compilation by the Rust compiler. */
public class CompilerCircuitStream extends CompilerCircuit {
    final InputOutputChangeStream stream;

    public CompilerCircuitStream(DBSPCompiler compiler, BaseSQLTests test) {
        this(compiler, new InputOutputChangeStream(), test);
    }

    public CompilerCircuitStream(DBSPCompiler compiler, BaseSQLTests test, String failureMessage) {
        this(compiler, new InputOutputChangeStream(), test, failureMessage);
    }

    public CompilerCircuitStream(
            DBSPCompiler compiler, List<String> inputs, List<String> outputs, BaseSQLTests test) {
        this(compiler, new InputOutputChangeStream(inputs, outputs), test);
    }

    public CompilerCircuitStream(DBSPCompiler compiler, InputOutputChangeStream streams, BaseSQLTests test) {
        super(compiler);
        this.stream = streams;
        test.addRustTestCase(this);
    }

    public CompilerCircuitStream(
            DBSPCompiler compiler, InputOutputChangeStream streams, BaseSQLTests test, String failureMessage) {
        super(compiler);
        this.stream = streams;
        test.addFailingRustTestCase(failureMessage, this);
    }

    /** Compiles a SQL script composed of INSERT statements.
     * into a Change. */
    public Change toChange(String script) {
        this.compiler.clearTables();
        this.compiler.submitStatementsForCompilation(script);
        TableContents tableContents = this.compiler.getTableContents();
        return new Change(tableContents);
    }

    /**
     * Add a step to a change stream with many input tables but one single output view.
     * A step is described as an input-output pair.
     *
     * @param script   SQL script that describes insertions and deletions into the input tables.
     * @param expected A text representation of the output produced for this step with an extra last
     *                 column that contains weights.
     */
    public void step(String script, String expected) {
        Change input = this.toChange(script);
        DBSPType outputType = this.circuit.getSingleOutputType();
        Change output = TableParser.parseChangeTable(expected, outputType);
        this.stream.addPair(input, output);
    }

    /** Execute some insert/delete statements using HSQLDB and compare the result
     * with the one produced by the circuit.
     * @param program program that creates the circuit.
     * @param script program that inserts data in tables */
    public void compareDB(String program, String script) throws ClassNotFoundException, SQLException {
        Class.forName("org.hsqldb.jdbcDriver");
        // a new instance of the database for each script
        String jdbcUrl = "jdbc:hsqldb:mem:db";
        Connection connection = DriverManager.getConnection(jdbcUrl, "", "");
        program += script;
        // not very robust
        String[] stats = program.split(";");
        for (String stat: stats) {
            try (Statement s = connection.createStatement()) {
                s.execute(stat);
            }
        }
        List<DBSPSinkOperator> sinks = Linq.where(
                Linq.list(circuit.sinkOperators.values()), o -> !o.metadata.system);
        String view = sinks.get(0).viewName.toString();
        String query = "SELECT * FROM " + view;
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            Change change = TableParser.fromResultSet(rs, this.circuit.getSingleOutputType());
            this.stream.addPair(this.toChange(script), change);
        }

        try (Statement s = connection.createStatement()) {
            s.execute("DROP SCHEMA PUBLIC CASCADE;");
        }
        try (Statement s = connection.createStatement()) {
            s.execute("SHUTDOWN;");
        }
        connection.close();
    }

    public void addChange(InputOutputChange ioChange) {
        this.stream.addChange(ioChange);
    }

    public void addPair(Change inputChange, Change outputChange) {
        this.stream.addPair(inputChange, outputChange);
    }
}

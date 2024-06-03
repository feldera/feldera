package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.sql.simple.Change;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.junit.Assert;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class that offers support for running SQL tests described as
 * setup + (query + expected_output)*
 *
 * <p>This class provides facilities for parsing the expected output
 * as produced by MySQL and Postgres.  The compiler-produced type for
 * the views is used to infer the types of values in columns, and the
 * types in turn drive the parsing of the values.
 *
 * <p>The main two methods are q() and qs(), which run respectively
 * one or multiple queries.
 */
public abstract class SqlIoTest extends BaseSQLTests {
    /** Override this method to prepare the tables on
     * which the tests are built. */
    public void prepareInputs(DBSPCompiler compiler) {}

    public CompilerOptions getOptions(boolean optimize) {
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.quiet = true;
        options.ioOptions.emitHandles = true;
        options.languageOptions.throwOnError = true;
        options.languageOptions.optimizationLevel = optimize ? 2 : 0;
        options.languageOptions.generateInputForEveryTable = true;
        options.languageOptions.incrementalize = false;
        return options;
    }

    public DBSPCompiler testCompiler(boolean optimize) {
        CompilerOptions options = this.getOptions(optimize);
        return new DBSPCompiler(options);
    }

    public DBSPCompiler compileQuery(String query, boolean optimize) {
        DBSPCompiler compiler = this.testCompiler(optimize);
        this.prepareInputs(compiler);
        compiler.compileStatement(query);
        if (!compiler.options.languageOptions.throwOnError) {
            compiler.throwIfErrorsOccurred();
        }
        return compiler;
    }

    /**
     * Generate and compile SQL to populate the specified table with data read from a resource file.
     * The resource file has the name table .csv.  This generates and compiles a bunch of "INSERT" statements.
     * We expect that the data is tab-separated.
     * @param table Table to populate.
     * @param compiler Compiler that processes the data.
     */
    public void insertFromResource(String table, DBSPCompiler compiler) {
        try {
            String data = TestUtil.readStringFromResourceFile(table + ".csv");
            String[] rows = data.split("\n");
            for (String row : rows) {
                String[] fields = row.split("\t");
                String insert = "INSERT INTO " + table + " VALUES('" +
                        String.join("', '", fields) + "')";
                compiler.compileStatement(insert);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** A Z-set that has the last column as a weight is converted into
     * a Z-set that uses the weight for the rest of the row.
     * For example, the Z-set:
     * (a, b, 2) -> 2
     * (c, d, -1) -> 1
     * Is converted into
     * (a, b) -> 4
     * (c, d) -> -1
     */
    static DBSPZSetLiteral extractWeight(DBSPZSetLiteral data) {
        DBSPTypeTuple rowType = data.getElementType().to(DBSPTypeTuple.class);
        int rowSize = rowType.size();
        DBSPType resultType = rowType.slice(0, rowSize - 1);
        DBSPZSetLiteral result = DBSPZSetLiteral.emptyWithElementType(resultType);

        for (Map.Entry<DBSPExpression, Long> entry: data.data.entrySet()) {
            Long weight = entry.getValue();
            DBSPExpression row = entry.getKey();
            DBSPTupleExpression tuple = row.to(DBSPTupleExpression.class);
            DBSPExpression[] prefix = new DBSPExpression[rowSize - 1];
            System.arraycopy(tuple.fields, 0, prefix, 0, prefix.length);
            DBSPExpression rowWeight = tuple.fields[rowSize - 1];
            weight *= rowWeight.to(DBSPI64Literal.class).value;
            DBSPExpression newRow = new DBSPTupleExpression(prefix);
            result.add(newRow, weight);
        }
        return result;
    }

    public Change getPreparedInputs(DBSPCompiler compiler) {
        DBSPZSetLiteral[] inputs = new DBSPZSetLiteral[
                compiler.getTableContents().tablesCreated.size()];
        int index = 0;
        for (String table: compiler.getTableContents().tablesCreated) {
            DBSPZSetLiteral data = compiler.getTableContents().getTableContents(table.toUpperCase());
            inputs[index++] = data;
        }
        return new Change(inputs);
    }

    public void compare(String query, DBSPZSetLiteral expected, boolean optimize) {
        DBSPCompiler compiler = this.testCompiler(optimize);
        this.prepareInputs(compiler);
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        if (!compiler.options.languageOptions.throwOnError)
            compiler.throwIfErrorsOccurred();
        compiler.optimize();
        InputOutputChange iochange = new InputOutputChange(
                this.getPreparedInputs(compiler),
                new Change(expected)
        );
        this.addRustTestCase(query, new CompilerCircuitStream(compiler, iochange.toStream()));
    }

    public void compare(String query, String expected, boolean optimize) {
        DBSPCompiler compiler = this.testCompiler(optimize);
        this.prepareInputs(compiler);
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        if (!compiler.options.languageOptions.throwOnError)
            compiler.throwIfErrorsOccurred();
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        ccs.showErrors();
        DBSPType outputType = ccs.circuit.getSingleOutputType();
        Change result = TableParser.parseTable(expected, outputType);
        InputOutputChange change = new InputOutputChange(
                this.getPreparedInputs(compiler),
                result
        );
        ccs.addChange(change);
        this.addRustTestCase(query, ccs);
    }

    String removeComments(String queryAndOutput) {
        String[] lines = queryAndOutput.split("\n");
        int index = 0;
        for (String line: lines) {
            line = line.trim();
            int comment = line.indexOf("--");
            if (comment >= 0) {
                line = line.substring(0, comment);
                lines[index] = line;
            }
            index++;
            if (line.contains(";"))
                break;
        }
        return String.join("\n", lines);
    }

    /**
     * Test a query followed by the expected output.
     * The query ends at the semicolon.
     * Runs two test cases, one with optimizations and one without.
     * This makes sure that constant queries still exercise the runtime.
     */
    public void q(String queryAndOutput, boolean twoWays) {
        queryAndOutput = this.removeComments(queryAndOutput);
        int semicolon = queryAndOutput.indexOf(';');
        if (semicolon < 0)
            throw new RuntimeException("Could not parse query and output");
        String query = queryAndOutput.substring(0, semicolon);
        String expected = queryAndOutput.substring(semicolon + 1);
        this.compare(query, expected, true);
        if (twoWays)
            this.compare(query, expected, false);
    }

    public void q(String queryAndOutput) {
        this.q(queryAndOutput, true);
    }

    /** Run a query that is expected to fail in compilation.
     * @param query             Query to run.
     * @param messageFragment   This fragment should appear in the error message. */
    public void queryFailingInCompilation(String query, String messageFragment) {
        this.statementFailingInCompilation("CREATE VIEW VV AS " + query, messageFragment);
    }

    /** Run a statement that is expected to fail in compilation.
     * @param statement         Statement to compile.
     * @param messageFragment   This fragment should appear in the error message. */
    public void statementFailingInCompilation(String statement, String messageFragment) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        this.prepareInputs(compiler);
        compiler.compileStatement(statement);
        compiler.optimize();
        Assert.assertTrue(compiler.messages.exitCode != 0);
        String message = compiler.messages.toString();
        boolean contains = message.contains(messageFragment);
        Assert.assertTrue(contains);
    }

    /** Run a query that is expected to fail in compilation.
     * @param query             Query to run.
     * @param messageFragment   This fragment should appear in the error message.
     * @param optimize          Boolean that indicates if the query should be compiled with optimizations. */
    public void queryFailingInCompilation(String query, String messageFragment, boolean optimize) {
        DBSPCompiler compiler = this.testCompiler(optimize);
        compiler.options.languageOptions.throwOnError = false;
        this.prepareInputs(compiler);
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        if (optimize) {
            compiler.optimize();
        }
        Assert.assertTrue(compiler.messages.exitCode != 0);
        String message = compiler.messages.toString();
        Assert.assertTrue(message.contains(messageFragment));
    }

    /**
     * Run a query that is expected to give a warning at compile time.
     * @param query             Query to run.
     * @param messageFragment   This fragment should appear in the warning message.
     */
    public void shouldWarn(String query, String messageFragment) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        this.prepareInputs(compiler);
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        compiler.optimize();
        Assert.assertTrue(compiler.hasWarnings);
        String warnings = compiler.messages.messages.stream().filter(error -> error.warning).toList().toString();
        Assert.assertTrue(warnings.contains(messageFragment));
    }

    /**
     * Test a sequence of queries, each followed by its expected output.
     * Two queries are separated by a whitespace line.
     * Here is an example legal input:
     * SELECT f.* FROM FLOAT4_TBL f WHERE f.f1 = '1004.3';
     *    f1
     * --------
     *  1004.3
     * (1 row)
     * <p>
     * SELECT f.* FROM FLOAT4_TBL f WHERE '1004.3' > f.f1;
     *       f1
     * ---------------
     *              0
     *         -34.84
     *  1.2345679e-20
     * (3 rows)
     */
    public void qs(String queriesWithOutputs, boolean twoWays) {
        String[] parts = queriesWithOutputs.split("\n\n");
        // From each part drop the last line (N rows) *and* its last newline.
        Pattern regex = Pattern.compile("^(.*)\\n\\(\\d+ rows?\\)$", Pattern.DOTALL);
        int index = 0;
        for (String part: parts) {
            Matcher regexMatcher = regex.matcher(part);
            if (regexMatcher.find()) {
                String result = regexMatcher.group(1);
                this.q(result, twoWays);
            } else {
                throw new RuntimeException("Could not understand test #" + index + ": " + part);
            }
            index++;
        }
    }

    public void qs(String queriesWithOutputs) {
        this.qs(queriesWithOutputs, true);
    }

    /**
     * Test the query for run time failure.
     * @param query         The query to run.
     * @param panicMessage  The fragment of string that should appear in the panic message.
     * @param optimize      Boolean that indicates if the query should be compiled with optimizations.
     */
    public void qf(String query, String panicMessage, boolean optimize) {
        DBSPCompiler compiler = this.testCompiler(optimize);
        this.prepareInputs(compiler);
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        if (!compiler.options.languageOptions.throwOnError)
            compiler.throwIfErrorsOccurred();
        CompilerCircuitStream ccs = new CompilerCircuitStream(compiler);
        DBSPCircuit circuit = ccs.circuit;
        DBSPType outputType = circuit.getSingleOutputType();
        Change result = new Change(
                DBSPZSetLiteral.emptyWithElementType(outputType.to(DBSPTypeZSet.class).getElementType()));
        InputOutputChange ioChange = new InputOutputChange(
                this.getPreparedInputs(compiler),
                result
        );
        ccs.addChange(ioChange);
        this.addFailingRustTestCase(query, panicMessage, ccs);
    }

    /**
     * Test the query for run time failure with and without optimizations enabled.
     * @param query         The query to run.
     * @param panicMessage  The fragment of string that should appear in the panic message.
     */
    public void qf(String query, String panicMessage) {
        this.qf(query, panicMessage, true);
        this.qf(query, panicMessage, false);
    }
}

package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Simplify;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.util.Utilities;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
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
    public enum TestOptimizations {
        Optimized,
        Unoptimized,
        Both
    }

    protected boolean optimize = true;
    
    public void setOptimize(boolean optimize) {
        this.optimize = optimize;
    }

    public CompilerOptions testOptions() {
        CompilerOptions options = new CompilerOptions();
        options.ioOptions.quiet = true;
        options.ioOptions.emitHandles = true;
        options.languageOptions.throwOnError = true;
        options.languageOptions.optimizationLevel = this.optimize ? 2 : 1;
        options.languageOptions.generateInputForEveryTable = true;
        options.languageOptions.incrementalize = false;
        options.languageOptions.unrestrictedIOTypes = true;
        options.ioOptions.verbosity = 2;
        return options;
    }

    public DBSPCompiler compileQuery(String query, boolean optimize) {
        this.setOptimize(optimize);
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementForCompilation(query);
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
                compiler.submitStatementForCompilation(insert);
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
    static DBSPZSetExpression extractWeight(DBSPZSetExpression data) {
        DBSPTypeTuple rowType = data.getElementType().to(DBSPTypeTuple.class);
        int rowSize = rowType.size();
        DBSPType resultType = rowType.slice(0, rowSize - 1);
        DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(resultType);

        for (Map.Entry<DBSPExpression, Long> entry: data.data.entrySet()) {
            Long weight = entry.getValue();
            DBSPExpression row = entry.getKey();
            DBSPTupleExpression tuple = row.to(DBSPTupleExpression.class);
            DBSPExpression[] prefix = new DBSPExpression[rowSize - 1];
            Assert.assertNotNull(tuple.fields);
            System.arraycopy(tuple.fields, 0, prefix, 0, prefix.length);
            DBSPExpression rowWeight = tuple.fields[rowSize - 1];
            weight *= rowWeight.to(DBSPI64Literal.class).value;
            DBSPExpression newRow = new DBSPTupleExpression(prefix);
            result.append(newRow, weight);
        }
        return result;
    }

    // Maps Rust representation to change
    static final HashMap<String, Change> cachedChangeList = new HashMap<>();

    static Change getCachedChange(DBSPCompiler compiler, DBSPZSetExpression[] data) {
        DBSPTupleExpression tuple = new DBSPTupleExpression(data);
        String string = ToRustInnerVisitor.toRustString(compiler, tuple, false);
        Change change = cachedChangeList.get(string);
        if (change != null)
            return change;
        Change result = new Change(data);
        Utilities.putNew(cachedChangeList, string, result);
        return result;
    }

    public Change getPreparedInputs(DBSPCompiler compiler) {
        DBSPZSetExpression[] inputs = new DBSPZSetExpression[
                compiler.getTableContents().tablesCreated.size()];
        int index = 0;
        Simplify simplify = new Simplify(compiler);
        for (ProgramIdentifier table: compiler.getTableContents().tablesCreated) {
            DBSPZSetExpression data = compiler.getTableContents().getTableContents(table);
            var simplified = simplify.apply(data);
            inputs[index++] = simplified.to(DBSPZSetExpression.class);
        }
        return getCachedChange(compiler, inputs);
    }

    public void compare(String query, DBSPZSetExpression expected, boolean optimize) {
        this.setOptimize(optimize);
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementForCompilation("CREATE VIEW VV AS " + query);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        ccs.showErrors();
        if (!compiler.options.languageOptions.throwOnError)
            compiler.throwIfErrorsOccurred();
        InputOutputChange iochange = new InputOutputChange(
                this.getPreparedInputs(compiler),
                new Change(expected)
        );
        ccs.addChange(iochange);
    }

    public void compare(String query, String expected, boolean optimize, int rowCount) {
        this.setOptimize(optimize);
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementForCompilation("CREATE VIEW VV AS " + query);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        ccs.showErrors();
        if (!compiler.options.languageOptions.throwOnError)
            compiler.throwIfErrorsOccurred();
        DBSPType outputType = ccs.circuit.getSingleOutputType();
        Change result = TableParser.parseTable(expected, outputType, rowCount);
        InputOutputChange change = new InputOutputChange(
                this.getPreparedInputs(compiler),
                result
        );
        ccs.addChange(change);
    }

    String removeComments(String queryAndOutput) {
        String[] lines = queryAndOutput.split("\n");
        int index = 0;
        for (String line: lines) {
            line = line.trim();
            boolean inString = false;
            for (int i = 0; i < line.length(); i++) {
                String c = line.substring(i, i + 1);
                if (c.equals("'")) {
                    inString = !inString;
                }
                if (inString)
                    continue;
                if (i < line.length() - 2) {
                    String two = line.substring(i, i + 2);
                    if (two.equals("--")) {
                        line = line.substring(0, i);
                        lines[index] = line;
                    }
                }
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
     *
     * @param rowCount Expected row count.  -1 if unknown.
     */
    public void q(String queryAndOutput, TestOptimizations to, int rowCount) {
        queryAndOutput = this.removeComments(queryAndOutput);
        int semicolon = queryAndOutput.indexOf(';');
        if (semicolon < 0)
            throw new RuntimeException("Could not parse query and output: " + queryAndOutput);
        String query = queryAndOutput.substring(0, semicolon);
        String expected = queryAndOutput.substring(semicolon + 1);
        if (to == TestOptimizations.Both || to == TestOptimizations.Optimized)
            this.compare(query, expected, true, rowCount);
        if (to == TestOptimizations.Both || to == TestOptimizations.Unoptimized)
            this.compare(query, expected, false, rowCount);
    }

    public void q(String queryAndOutput) {
        this.q(queryAndOutput, TestOptimizations.Both, -1);
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
     *
     * <p>SELECT f.* FROM FLOAT4_TBL f WHERE '1004.3' > f.f1;
     *       f1
     * ---------------
     *              0
     *         -34.84
     *  1.2345679e-20
     * (3 rows)
     */
    protected void qs(String queriesWithOutputs, TestOptimizations to) {
        String[] parts = queriesWithOutputs.split("\n\n");
        // From each part drop the last line (N rows) *and* its last newline.
        Pattern regex = Pattern.compile("^(.*)\\n\\((\\d+) rows?\\)$", Pattern.DOTALL);
        int index = 0;
        for (String part: parts) {
            Matcher regexMatcher = regex.matcher(part);
            if (regexMatcher.find()) {
                String queryAndOutput = regexMatcher.group(1);
                String rows = regexMatcher.group(2);
                int rowCount = Integer.parseInt(rows);
                this.q(queryAndOutput, to, rowCount);
            } else {
                throw new RuntimeException("Could not understand test #" + index + ": " + part);
            }
            index++;
        }
    }

    public void qs(String queriesWithOutputs) {
        this.qs(queriesWithOutputs, TestOptimizations.Both);
    }

    /**
     * Test the query for run time failure.
     * @param query         The query to run.
     * @param panicMessage  The fragment of string that should appear in the panic message.
     * @param optimize      Boolean that indicates if the query should be compiled with optimizations.
     */
    public void qf(String query, String panicMessage, boolean optimize) {
        this.setOptimize(optimize);
        DBSPCompiler compiler = this.testCompiler();
        this.prepareInputs(compiler);
        compiler.submitStatementForCompilation("CREATE VIEW VV AS " + query);
        if (!compiler.options.languageOptions.throwOnError)
            compiler.throwIfErrorsOccurred();
        CompilerCircuitStream ccs = this.getCCSFailing(compiler, panicMessage);
        DBSPCircuit circuit = ccs.circuit;
        DBSPType outputType = circuit.getSingleOutputType();
        Change result = new Change(
                DBSPZSetExpression.emptyWithElementType(outputType.to(DBSPTypeZSet.class).getElementType()));
        InputOutputChange ioChange = new InputOutputChange(
                this.getPreparedInputs(compiler),
                result
        );
        ccs.addChange(ioChange);
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

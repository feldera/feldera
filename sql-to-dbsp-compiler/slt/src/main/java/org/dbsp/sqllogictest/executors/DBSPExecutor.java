/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqllogictest.executors;

import net.hydromatic.sqllogictest.ISqlTestOperation;
import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.SltSqlStatement;
import net.hydromatic.sqllogictest.SltTestFile;
import net.hydromatic.sqllogictest.SqlTestQuery;
import net.hydromatic.sqllogictest.SqlTestQueryOutputDescription;
import net.hydromatic.sqllogictest.TestStatistics;
import net.hydromatic.sqllogictest.executors.SqlSltTestExecutor;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPExpressionStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.*;
import org.dbsp.sqlCompiler.ir.type.primitive.*;
import org.dbsp.sqllogictest.*;
import org.dbsp.util.Linq;
import org.dbsp.util.ProgramAndTester;
import org.dbsp.util.TableValue;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sql test executor that uses DBSP for query execution.
 */
public class DBSPExecutor extends SqlSltTestExecutor {
    private final boolean execute;
    public final CompilerOptions compilerOptions;

    private final String connectionString; // either csv or a valid sqlx connection string
    final SqlTestPrepareInput inputPreparation;
    final SqlTestPrepareTables tablePreparation;
    final SqlTestPrepareViews viewPreparation;
    private final List<SqlTestQuery> queriesToRun;

    public int toSkip = 0;

    /**
     * Create an executor that executes SqlLogicTest queries directly compiling to
     * Rust and using the DBSP library.
     * @param connectionString Connection string to use to get ground truth from database.
     * @param options  Options to use for compilation.
     */
    public DBSPExecutor(OptionsParser.SuppliedOptions options,
                        CompilerOptions compilerOptions, String connectionString) {
        super(options);
        this.execute = !options.doNotExecute;
        this.inputPreparation = new SqlTestPrepareInput();
        this.tablePreparation = new SqlTestPrepareTables();
        this.viewPreparation = new SqlTestPrepareViews();
        this.compilerOptions = compilerOptions;
        this.queriesToRun = new ArrayList<>();
        this.connectionString = connectionString;
    }

    public void skip(int toSkip) {
        this.toSkip = toSkip;
    }

    public TableValue[] getInputSets(DBSPCompiler compiler) throws SQLException {
        for (SltSqlStatement statement : this.inputPreparation.statements)
            compiler.compileStatement(statement.statement, null);
        TableContents tables = compiler.getTableContents();
        TableValue[] tableValues = new TableValue[tables.tablesCreated.size()];
        for (int i = 0; i < tableValues.length; i++) {
            String table = tables.tablesCreated.get(i);
            tableValues[i] = new TableValue(table,
                    new DBSPZSetLiteral(new DBSPTypeWeight(), tables.getTableContents(table)));
        }
        return tableValues;
    }

    class ExecutorInputGenerator implements InputGenerator {
        final DBSPCompiler compiler;

        ExecutorInputGenerator(DBSPCompiler compiler) {
            this.compiler = compiler;
        }

        @Override
        public TableValue[] getInputs() throws SQLException {
            return DBSPExecutor.this.getInputSets(this.compiler);
        }
    }

    boolean runBatch(TestStatistics result, boolean cleanup) {
        try {
            final List<ProgramAndTester> codeGenerated = new ArrayList<>();

            // Create contents for input tables
            DBSPCompiler compiler = new DBSPCompiler(this.compilerOptions);
            this.createTables(compiler);
            compiler.throwIfErrorsOccurred();
            // Create function which generates inputs for all tests in this batch.
            // We know that all these tests consume the same input tables.
            ExecutorInputGenerator egen = new ExecutorInputGenerator(compiler);
            InputFunctionGenerator gen = new InputFunctionGenerator(compiler, egen, this.connectionString);

            // Generate a function and a tester for each query.
            int queryNo = 0;
            for (SqlTestQuery testQuery : this.queriesToRun) {
                try {
                    // Create input tables in circuit
                    if (queryNo > 0) {
                        // Allocate a fresh compiler
                        compiler = new DBSPCompiler(this.compilerOptions);
                        this.createTables(compiler);
                    }
                    ProgramAndTester pc = this.generateTestCase(
                            compiler, gen, this.viewPreparation, testQuery, queryNo);
                    codeGenerated.add(pc);
                } catch (Throwable ex) {
                    System.err.println("Error while compiling " + testQuery.getQuery() + ": " +
                            ex.getClass().getSimpleName() + " " + ex.getMessage());
                    result.addFailure(
                            new TestStatistics.FailedTestDescription(testQuery,
                                    "Exception during test", "", ex));
                    throw ex;
                    // return false;
                }
                queryNo++;
            }

            // Write the code to Rust files on the filesystem.
            DBSPFunction inputFunction = gen.getInputFunction();
            DBSPFunction streamInputFunction = gen.getStreamInputFunction();
            this.writeCodeToFile(compiler,
                    Linq.list(inputFunction, streamInputFunction), codeGenerated);
            this.startTest();
            if (this.execute) {
                String[] extraArgs = new String[0];
                if (this.compilerOptions.ioOptions.jit) {
                    extraArgs = new String[2];
                    extraArgs[0] = "--features";
                    extraArgs[1] = "jit";
                }
                Utilities.compileAndTestRust(Main.rustDirectory, true, extraArgs);
            }
            this.queriesToRun.clear();
            System.out.println(elapsedTime(queryNo));
            if (cleanup)
                this.cleanupFilesystem();
            if (this.execute)
                // This is not entirely correct, but I am not parsing the rust output
                result.setPassedTestCount(result.getPassedTestCount() + queryNo);
            else
                result.setIgnoredTestCount(result.getIgnoredTestCount() + queryNo);
        } catch (IOException | InterruptedException | SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    /**
     * Convert a description of the data in the SLT format to a ZSet.
     */
    @Nullable public static DBSPZSetLiteral convert(@Nullable List<String> data, DBSPTypeZSet outputType) {
        if (data == null)
            return null;
        DBSPZSetLiteral result;
        IDBSPContainer container;
        DBSPType elementType = outputType.elementType;
        if (elementType.is(DBSPTypeVec.class)) {
            elementType = elementType.to(DBSPTypeVec.class).getElementType();
            DBSPVecLiteral vec = new DBSPVecLiteral(elementType);
            container = vec;
            result = new DBSPZSetLiteral(new DBSPTypeWeight(), vec);
        } else {
            result = new DBSPZSetLiteral(outputType.getElementType(), new DBSPTypeWeight());
            container = result;
        }
        DBSPTypeTuple outputElementType = elementType.to(DBSPTypeTuple.class);

        List<DBSPExpression> fields = new ArrayList<>();
        int col = 0;
        DBSPExpression field;
        for (String s : data) {
            DBSPType colType = outputElementType.tupFields[col];
            if (s.equalsIgnoreCase("null"))
                field = DBSPLiteral.none(colType);
            else if (colType.is(DBSPTypeInteger.class))
                field = new DBSPI32Literal(Integer.parseInt(s));
            else if (colType.is(DBSPTypeDouble.class))
                field = new DBSPDoubleLiteral(Double.parseDouble(s));
            else if (colType.is(DBSPTypeReal.class))
                field = new DBSPRealLiteral(Float.parseFloat(s));
            else if (colType.is(DBSPTypeString.class))
                field = new DBSPStringLiteral(s);
            else if (colType.is(DBSPTypeDecimal.class))
                field = new DBSPDecimalLiteral(colType, new BigDecimal(s));
            else if (colType.is(DBSPTypeBool.class))
                // Booleans are encoded as ints
                field = new DBSPBoolLiteral(Integer.parseInt(s) != 0);
            else
                throw new RuntimeException("Unexpected type " + colType);
            if (!colType.sameType(field.getType()))
                field = field.cast(colType);
            fields.add(field);
            col++;
            if (col == outputElementType.size()) {
                container.add(new DBSPTupleExpression(CalciteObject.EMPTY, fields));
                fields = new ArrayList<>();
                col = 0;
            }
        }
        if (col != 0) {
            throw new RuntimeException("Could not assign all query output values to rows. " +
                    "I have " + col + " leftover values in the last row");
        }
        return result;
    }

    ProgramAndTester generateTestCase(
            DBSPCompiler compiler,
            InputFunctionGenerator gen,
            SqlTestPrepareViews viewPreparation,
            SqlTestQuery testQuery, int suffix) throws IOException, SQLException {
        String origQuery = testQuery.getQuery();
        String dbspQuery = origQuery;
        if (!dbspQuery.toLowerCase().contains("create view"))
            dbspQuery = "CREATE VIEW V" + suffix + " AS (" + origQuery + ")";
        this.options.message("Query " + suffix + ":\n"
                        + dbspQuery + "\n", 2);
        compiler.generateOutputForNextView(false);
        for (SltSqlStatement view: viewPreparation.definitions()) {
            compiler.compileStatement(view.statement, view.statement);
            compiler.throwIfErrorsOccurred();
        }
        compiler.generateOutputForNextView(true);
        compiler.compileStatement(dbspQuery, "" /* testQuery.getName() */);
        compiler.throwIfErrorsOccurred();
        compiler.optimize();
        DBSPCircuit dbsp = compiler.getFinalCircuit("gen" + suffix);
        DBSPNode.done();

        if (dbsp.getOutputCount() != 1)
            throw new RuntimeException(
                    "Didn't expect a query to have " + dbsp.getOutputCount() + " outputs");
        DBSPTypeZSet outputType = dbsp.getOutputType(0).to(DBSPTypeZSet.class);
        DBSPZSetLiteral expectedOutput = DBSPExecutor.convert(
                testQuery.outputDescription.getQueryResults(), outputType);
        if (expectedOutput == null) {
            if (testQuery.outputDescription.hash == null)
                throw new RuntimeException("No hash or outputs specified");
        }

        return createTesterCode(
                    "tester" + suffix, dbsp,
                    gen.getStreamInputFunction(),
                    compiler.getTableContents(),
                    expectedOutput, testQuery.outputDescription);
    }

    void cleanupFilesystem() {
        File directory = new File(Main.rustDirectory);
        FilenameFilter filter = (dir, name) -> name.startsWith(Main.testFileName) || name.endsWith("csv");
        File[] files = directory.listFiles(filter);
        if (files == null)
            return;
        for (File file: files) {
            boolean deleted = file.delete();
            if (!deleted)
                throw new RuntimeException("Cannot delete file " + file);
        }
    }

    void createTables(DBSPCompiler compiler) {
        for (SltSqlStatement statement : this.tablePreparation.statements) {
            String stat = statement.statement;
            compiler.compileStatement(stat, stat);
        }
    }

    @Override
    public TestStatistics execute(SltTestFile file, OptionsParser.SuppliedOptions options)
            throws SQLException {
        this.startTest();
        int batchSize = 1;
        String name = file.toString();
        if (name.contains("/"))
            name = name.substring(name.lastIndexOf('/') + 1);
        if (name.startsWith("select"))
            batchSize = 20;
        if (name.startsWith("select5"))
            batchSize = 5;
        if (this.toSkip > 0)
            batchSize = 1;

        TestStatistics result = new TestStatistics(options.stopAtFirstError, options.verbosity);
        boolean seenQueries = false;
        int remainingInBatch = batchSize;
        boolean skipped = this.toSkip > 0;
        for (ISqlTestOperation operation: file.fileContents) {
            SltSqlStatement stat = operation.as(SltSqlStatement.class);
            if (stat != null) {
                if (seenQueries) {
                    boolean success = this.runBatch(result, !skipped);
                    if (options.stopAtFirstError && !success)
                        return result;
                    remainingInBatch = batchSize;
                    seenQueries = false;
                }
                try {
                    if (this.buggyOperations.contains(stat.statement)) {
                        this.options.message("Skipping buggy test " + stat.statement + "\n", 1);
                    } else {
                        this.statement(stat);
                        if (!stat.shouldPass) {
                            options.err.println("Statement should have failed: " + operation);
                        }
                    }
                } catch (SQLException ex) {
                    if (stat.shouldPass)
                        this.options.error(ex);
                }
                this.statementsExecuted++;
            } else {
                SqlTestQuery query = operation.to(options.err, SqlTestQuery.class);
                if (this.buggyOperations.contains(query.getQuery())) {
                    options.message("Skipping " + query.getQuery(), 2);
                    result.incIgnored();
                    continue;
                }
                if (this.toSkip > 0) {
                    this.toSkip--;
                    continue;
                }
                seenQueries = true;
                this.queriesToRun.add(query);
                remainingInBatch--;
                if (remainingInBatch == 0) {
                    boolean success = this.runBatch(result, !skipped);
                    if (!success && options.stopAtFirstError)
                        return result;
                    remainingInBatch = batchSize;
                    seenQueries = false;
                }
                if (skipped)
                    // stop after the first test
                    return result;
            }
        }
        if (remainingInBatch != batchSize)
            this.runBatch(result, !skipped);
        // Make sure there are no left-overs if this executor
        // is invoked to process a new file.
        this.reset();
        return result;
    }

    /**
     * Generates a Rust function which tests a DBSP circuit.
     * @param name        Name of the generated function.
     * @param circuit     DBSP circuit that will be tested.
     * @param output      Expected data from the circuit.
     * @param description Description of the expected outputs.
     * @return The code for a function that runs the circuit with the specified
     * input and tests the produced output.
     */
    static ProgramAndTester createTesterCode(
            String name,
            DBSPCircuit circuit,
            DBSPFunction inputGeneratingFunction,
            TableContents contents,
            @Nullable DBSPZSetLiteral output,
            SqlTestQueryOutputDescription description) {
        List<DBSPStatement> list = new ArrayList<>();
        DBSPLetStatement circ = new DBSPLetStatement("circ",
                new DBSPApplyExpression(circuit.getNode(), circuit.name, DBSPTypeAny.getDefault()), true);
        list.add(circ);
        DBSPType circuitOutputType = circuit.getOutputType(0);
        // the following may not be the same, since SqlLogicTest sometimes lies about the output type
        DBSPTypeRawTuple outputType = new DBSPTypeRawTuple(output != null ? output.getType() : circuitOutputType);
        DBSPExpression[] arguments = new DBSPExpression[circuit.getInputTables().size()];
        // True if the output is a zset of vectors (generated for orderby queries)
        boolean isVector = circuitOutputType.to(DBSPTypeZSet.class).elementType.is(DBSPTypeVec.class);

        DBSPLetStatement inputStream = new DBSPLetStatement("_in_stream",
                inputGeneratingFunction.call());
        list.add(inputStream);
        for (int i = 0; i < arguments.length; i++) {
            String inputI = circuit.getInputTables().get(i);
            int index = contents.getTableIndex(inputI);
            arguments[i] = DBSPTypeAny.getDefault().var("_in").field(index);
        }
        DBSPLetStatement createOutput = new DBSPLetStatement(
                "output",
                new DBSPRawTupleExpression(
                        new DBSPApplyExpression(
                                "zset!", outputType.getFieldType(0))), true);
        list.add(createOutput);
        DBSPForExpression loop = new DBSPForExpression(
                new DBSPIdentifierPattern("_in"),
                inputStream.getVarReference(),
                new DBSPBlockExpression(
                        Linq.list(),
                        new DBSPAssignmentExpression(createOutput.getVarReference(),
                        new DBSPApplyExpression("add_zset_tuple", outputType,
                        createOutput.getVarReference(),
                        new DBSPApplyExpression("circ", outputType, arguments)))
                )
        );
        list.add(new DBSPExpressionStatement(loop));
        DBSPExpression sort = new DBSPEnumValue("SortOrder", description.getOrder().toString());
        DBSPExpression output0 = createOutput.getVarReference().field(0);

        if (description.getExpectedOutputSize() >= 0) {
            DBSPExpression count;
            if (isVector) {
                count = new DBSPApplyExpression(
                        "weighted_vector_count",
                        new DBSPTypeUSize(CalciteObject.EMPTY, false),
                        output0.borrow());
            } else {
                count = new DBSPApplyMethodExpression("weighted_count",
                        new DBSPTypeUSize(CalciteObject.EMPTY, false), output0);
            }
            list.add(new DBSPExpressionStatement(
                    new DBSPApplyExpression("assert_eq!", new DBSPTypeVoid(),
                            new DBSPAsExpression(count, new DBSPTypeWeight()), new DBSPAsExpression(
                                    new DBSPI32Literal(description.getExpectedOutputSize()),
                            new DBSPTypeWeight()))));
        }
        if (output != null) {
            if (description.columnTypes != null) {
                DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
                DBSPTypeZSet oType = output.getType().to(DBSPTypeZSet.class);
                String functionProducingStrings;
                DBSPType elementType;
                if (isVector) {
                    functionProducingStrings = "zset_of_vectors_to_strings";
                    elementType = oType.elementType.to(DBSPTypeVec.class).getElementType();
                } else {
                    functionProducingStrings = "zset_to_strings";
                    elementType = oType.elementType;
                }
                DBSPExpression zset_to_strings = new DBSPQualifyTypeExpression(
                        DBSPTypeAny.getDefault().var(functionProducingStrings),
                        elementType,
                        oType.weightType
                );
                list.add(new DBSPExpressionStatement(
                        new DBSPApplyExpression("assert_eq!", new DBSPTypeVoid(),
                                new DBSPApplyExpression(functionProducingStrings, DBSPTypeAny.getDefault(),
                                        output0.borrow(), columnTypes, sort),
                                 zset_to_strings.call(output.borrow(), columnTypes, sort))));
            } else {
                list.add(new DBSPExpressionStatement(new DBSPApplyExpression(
                        "assert_eq!", new DBSPTypeVoid(), output0, output)));
            }
        } else {
            if (description.columnTypes == null)
                throw new RuntimeException("Expected column types to be supplied");
            DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
            if (description.hash == null)
                throw new RuntimeException("Expected hash to be supplied");
            String hash = isVector ? "hash_vectors" : "hash";
            list.add(new DBSPLetStatement("_hash",
                    new DBSPApplyExpression(hash, new DBSPTypeString(CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, false),
                            output0.borrow(),
                            columnTypes,
                            sort)));
            list.add(
                    new DBSPExpressionStatement(
                            new DBSPApplyExpression("assert_eq!", new DBSPTypeVoid(),
                                    new DBSPTypeString(CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, false).var("_hash"),
                                    new DBSPStringLiteral(description.hash))));
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction function = new DBSPFunction(name, new ArrayList<>(), new DBSPTypeVoid(), body, Linq.list("#[test]"));
        return new ProgramAndTester(circuit, function);
    }

    public boolean statement(SltSqlStatement statement) throws SQLException {
        this.options.message("Executing " + statement + "\n", 2);
        String command = statement.statement.toLowerCase();
        if (command.startsWith("create index"))
            return true;
        if (command.startsWith("create distinct index"))
            return false;
        if (command.contains("create table") || command.contains("drop table")) {
            this.tablePreparation.add(statement);
        } else if (command.contains("create view")) {
            this.viewPreparation.add(statement);
        } else if (command.contains("drop view")) {
            this.viewPreparation.remove(statement);
        } else
            this.inputPreparation.add(statement);
        return true;
    }

    void reset() {
        this.inputPreparation.clear();
        this.tablePreparation.clear();
        this.viewPreparation.clear();
        this.queriesToRun.clear();
    }

    public void writeCodeToFile(
            DBSPCompiler compiler,
            List<DBSPFunction> inputFunctions,
            List<ProgramAndTester> functions
    ) throws FileNotFoundException, UnsupportedEncodingException {
        String genFileName = Main.testFileName + ".rs";
        String testFilePath = Main.rustDirectory + "/" + genFileName;
        PrintStream stream = new PrintStream(testFilePath, "UTF-8");
        RustFileWriter rust = new RustFileWriter(compiler, stream);

        if (!compiler.options.ioOptions.jit) {
            for (DBSPFunction function : inputFunctions)
                rust.add(function);
        }
        for (ProgramAndTester pt: functions)
            rust.add(pt);
        rust.writeAndClose();
    }

    public static void register(OptionsParser parser, AtomicReference<Integer> skip) {
        AtomicReference<Boolean> incremental = new AtomicReference<>();
        incremental.set(false);
        parser.registerOption("-inc", null, "Incremental validation", o -> {
            incremental.set(true);
            return true;
        });
        parser.registerExecutor("dbsp", () -> {
            OptionsParser.SuppliedOptions options = parser.getOptions();
            try {
                CompilerOptions compilerOptions = new CompilerOptions();
                compilerOptions.languageOptions.incrementalize = incremental.get();
                compilerOptions.languageOptions.throwOnError = options.stopAtFirstError;
                compilerOptions.languageOptions.lenient = true;
                compilerOptions.languageOptions.generateInputForEveryTable = true;
                DBSPExecutor result = new DBSPExecutor(options, compilerOptions, "csv");
                result.skip(skip.get());
                Set<String> bugs = options.readBugsFile();
                result.avoid(bugs);
                return result;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}

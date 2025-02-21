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
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.frontend.TableContents;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPEnumValue;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPQualifyTypeExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.IDBSPContainer;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUSizeLiteral;
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPComment;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
import org.dbsp.sqllogictest.Main;
import org.dbsp.sqllogictest.SqlTestPrepareInput;
import org.dbsp.sqllogictest.SqlTestPrepareTables;
import org.dbsp.sqllogictest.SqlTestPrepareViews;
import org.dbsp.util.Linq;
import org.dbsp.util.ProgramAndTester;
import org.dbsp.util.TableValue;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Sql test executor that uses DBSP for query execution. */
public class DBSPExecutor extends SqlSltTestExecutor {
    private final boolean execute;
    public final CompilerOptions compilerOptions;

    final SqlTestPrepareInput inputPreparation;
    final SqlTestPrepareTables tablePreparation;
    final SqlTestPrepareViews viewPreparation;
    private final List<SqlTestQuery> queriesToRun;

    public int toSkip = 0;

    /**
     * Create an executor that executes SqlLogicTest queries directly compiling to
     * Rust and using the DBSP library.
     * @param options  Options to use for compilation.
     */
    public DBSPExecutor(OptionsParser.SuppliedOptions options,
                        CompilerOptions compilerOptions) {
        super(options);
        this.execute = !options.doNotExecute;
        this.inputPreparation = new SqlTestPrepareInput();
        this.tablePreparation = new SqlTestPrepareTables();
        this.viewPreparation = new SqlTestPrepareViews();
        this.compilerOptions = compilerOptions;
        this.queriesToRun = new ArrayList<>();
    }

    public void skip(int toSkip) {
        this.toSkip = toSkip;
    }

    public TableValue[] getInputSets(DBSPCompiler compiler) throws SQLException {
        for (SltSqlStatement statement : this.inputPreparation.statements)
            compiler.submitStatementForCompilation(statement.statement);
        TableContents tables = compiler.getTableContents();
        TableValue[] tableValues = new TableValue[tables.tablesCreated.size()];
        for (int i = 0; i < tableValues.length; i++) {
            ProgramIdentifier table = tables.tablesCreated.get(i);
            tableValues[i] = new TableValue(table, tables.getTableContents(table));
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
            InputFunctionGenerator gen = new InputFunctionGenerator(compiler, egen);

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
            this.writeCodeToFile(compiler, Linq.list(inputFunction), codeGenerated);
            this.startTest();
            if (this.execute) {
                Utilities.compileAndTestRust(Main.getAbsoluteRustDirectory(), true);
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

    /** Convert a description of the data in the SLT format to a ZSet. */
    public static DBSPZSetExpression convert(@Nullable List<String> data, DBSPTypeZSet outputType) {
        if (data == null)
            data = Linq.list();
        DBSPZSetExpression result;
        IDBSPContainer container;
        DBSPType elementType = outputType.elementType;
        if (elementType.is(DBSPTypeArray.class)) {
            elementType = elementType.to(DBSPTypeArray.class).getElementType();
            DBSPArrayExpression vec = DBSPArrayExpression.emptyWithElementType(elementType, false);
            container = vec;
            result = new DBSPZSetExpression(vec);
        } else {
            result = DBSPZSetExpression.emptyWithElementType(outputType.getElementType());
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
                field = field.cast(colType, false);
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
        for (SltSqlStatement view: viewPreparation.definitions()) {
            compiler.submitStatementForCompilation(view.statement
                    .replaceAll("(?i)create\\s+view", "create local view"));
            compiler.throwIfErrorsOccurred();
        }
        compiler.submitStatementForCompilation(dbspQuery);
        compiler.throwIfErrorsOccurred();
        DBSPCircuit circuit = compiler.getFinalCircuit(false);
        assert circuit != null;
        circuit.setName("circuit" + suffix);
        DBSPNode.done();

        List<DBSPSinkOperator> sinks = Linq.list(circuit.sinkOperators.values());
        int outputNumber = 0;
        DBSPSinkOperator sink = null;
        for (; outputNumber < circuit.getOutputCount(); outputNumber++) {
            sink = sinks.get(outputNumber);
            // Skip over system views if we are not checking these
            if (!sink.metadata.system)
                break;
        }
        assert sink != null;
        DBSPTypeZSet outputType = sink.outputType.to(DBSPTypeZSet.class);
        DBSPZSetExpression expectedOutput = null;
        if (testQuery.outputDescription.hash == null) {
            expectedOutput = DBSPExecutor.convert(testQuery.outputDescription.getQueryResults(), outputType);
        }

        return createTesterCode(
                    "tester" + suffix, origQuery, circuit, outputNumber,
                    gen.getInputFunction(),
                    compiler.getTableContents(),
                    expectedOutput, testQuery.outputDescription);
    }

    void cleanupFilesystem() {
        File directory = new File(Main.getAbsoluteRustDirectory());
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
            compiler.submitStatementForCompilation(stat);
        }
    }

    @Override
    public TestStatistics execute(SltTestFile file, OptionsParser.SuppliedOptions options)
            throws SQLException {
        this.startTest();
        int batchSize = 500;
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
     * @param query       Query that is being tested.
     * @param circuit     DBSP circuit that will be tested.
     * @param output      Expected data from the circuit.
     * @param description Description of the expected outputs.
     * @param outputNumber  Position of data output in output vector.
     * @return The code for a function that runs the circuit with the specified
     * input and tests the produced output. */
    static ProgramAndTester createTesterCode(
            String name,
            String query,
            DBSPCircuit circuit,
            int outputNumber,
            DBSPFunction inputGeneratingFunction,
            TableContents contents,
            @Nullable DBSPZSetExpression output,
            SqlTestQueryOutputDescription description) {
        List<DBSPStatement> list = new ArrayList<>();
        DBSPComment comment = new DBSPComment(query);
        list.add(comment);
        DBSPExpression arg = new DBSPApplyExpression(
                "CircuitConfig::with_workers", DBSPTypeAny.getDefault(), new DBSPUSizeLiteral(2)); // workers
        DBSPLetStatement cas = new DBSPLetStatement("circ",
                new DBSPApplyExpression(
                        circuit.getNode(), circuit.getName(), DBSPTypeAny.getDefault(), arg).resultUnwrap(), true);
        list.add(cas);
        DBSPLetStatement streams = new DBSPLetStatement("streams", cas.getVarReference().field(1));
        list.add(streams);

        List<DBSPSinkOperator> sinks = Linq.list(circuit.sinkOperators.values());
        DBSPSinkOperator sink = sinks.get(outputNumber);
        DBSPType circuitOutputType = sink.outputType;
        // True if the output is a zset of vectors (generated for order by queries)
        boolean isVector = circuitOutputType.to(DBSPTypeZSet.class).elementType.is(DBSPTypeArray.class);

        DBSPLetStatement inputStream = new DBSPLetStatement("_in",
                inputGeneratingFunction.call());
        list.add(inputStream);
        DBSPVariablePath loopIndex = new DBSPVariablePath("_in", DBSPTypeAny.getDefault());
        int i = 0;
        for (ProgramIdentifier inputI: circuit.getInputTables()) {
            int index = contents.getTableIndex(inputI);
            list.add(new DBSPApplyExpression("append_to_collection_handle", DBSPTypeAny.getDefault(),
                    loopIndex.field(index).borrow(),
                    streams.getVarReference().field(i).borrow())
                    .toStatement());
            i++;
        }
        DBSPLetStatement step =
                new DBSPLetStatement("_",
                        new DBSPApplyMethodExpression("step", DBSPTypeAny.getDefault(),
                        cas.getVarReference().field(0)).resultUnwrap());
        list.add(step);
        DBSPLetStatement outputStatement =
                new DBSPLetStatement("out",
                        new DBSPApplyExpression("read_output_handle", DBSPTypeAny.getDefault(),
                                streams.getVarReference().field(circuit.getInputTables().size() + outputNumber).borrow()));
        list.add(outputStatement);
        DBSPExpression sort = new DBSPEnumValue("SortOrder", description.getOrder().toString());

        /*
        This stopped working when dynamic types were merged.
        if (description.getExpectedOutputSize() >= 0) {
            DBSPExpression count;
            if (isVector) {
                count = new DBSPApplyExpression(
                        "weighted_vector_count",
                        new DBSPTypeUSize(CalciteObject.EMPTY, false),
                        outputStatement.getVarReference().borrow());
            } else {
                count = new DBSPApplyMethodExpression("weighted_count",
                        new DBSPTypeUSize(CalciteObject.EMPTY, false),
                        outputStatement.getVarReference());
            }
            list.add(new DBSPApplyExpression("assert_eq!", DBSPTypeVoid.INSTANCE,
                            new DBSPAsExpression(count,
                                    new DBSPTypeInteger(CalciteObject.EMPTY, 32, true, false)),
                                    new DBSPI32Literal(description.getExpectedOutputSize())).toStatement());
        }
         */
        if (output != null) {
            if (description.columnTypes != null) {
                DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
                DBSPTypeZSet oType = output.getType().to(DBSPTypeZSet.class);
                String functionProducingStrings;
                DBSPType elementType;
                if (isVector) {
                    functionProducingStrings = "zset_of_vectors_to_strings";
                    elementType = oType.elementType.to(DBSPTypeArray.class).getElementType();
                } else {
                    functionProducingStrings = "zset_to_strings";
                    elementType = oType.elementType;
                }
                DBSPExpression zset_to_strings = new DBSPQualifyTypeExpression(
                        new DBSPVariablePath(functionProducingStrings, DBSPTypeAny.getDefault()),
                        elementType
                );
                list.add(new DBSPApplyExpression("assert_eq!", DBSPTypeVoid.INSTANCE,
                                new DBSPApplyExpression(functionProducingStrings, DBSPTypeAny.getDefault(),
                                        outputStatement.getVarReference().borrow(), columnTypes, sort),
                                 zset_to_strings.call(output.borrow(), columnTypes, sort)).toStatement());
            } else {
                list.add(new DBSPApplyExpression(
                        "assert_eq!", DBSPTypeVoid.INSTANCE, outputStatement.getVarReference().borrow(), output)
                        .toStatement());
            }
        } else {
            if (description.columnTypes == null)
                throw new RuntimeException("Expected column types to be supplied");
            DBSPExpression columnTypes = new DBSPStringLiteral(description.columnTypes);
            if (description.hash == null)
                throw new RuntimeException("Expected hash to be supplied");
            String hash = isVector ? "hash_vectors" : "hash";
            DBSPVariablePath var = new DBSPTypeString(CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, false).var();
            list.add(new DBSPLetStatement(var.variable,
                    new DBSPApplyExpression(hash, new DBSPTypeString(CalciteObject.EMPTY, DBSPTypeString.UNLIMITED_PRECISION, false, false),
                            outputStatement.getVarReference().borrow(),
                            columnTypes,
                            sort)));
            list.add(new DBSPApplyExpression("assert_eq!", DBSPTypeVoid.INSTANCE,
                    var, new DBSPStringLiteral(description.hash)).toStatement());
        }
        DBSPExpression body = new DBSPBlockExpression(list, null);
        DBSPFunction function = new DBSPFunction(name, new ArrayList<>(), DBSPTypeVoid.INSTANCE, body, Linq.list("#[test]"));
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
    ) throws IOException {
        String genFileName = Main.testFileName + ".rs";
        String testFilePath = Main.getAbsoluteRustDirectory() + "/" + genFileName;
        PrintStream stream = new PrintStream(testFilePath, StandardCharsets.UTF_8);
        RustFileWriter rust = new RustFileWriter(stream).forSlt();

        for (DBSPFunction function : inputFunctions)
            rust.add(function);
        for (ProgramAndTester pt: functions)
            rust.add(pt);
        rust.writeAndClose(compiler);
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
                compilerOptions.ioOptions.verbosity = options.verbosity;
                DBSPExecutor result = new DBSPExecutor(options, compilerOptions);
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

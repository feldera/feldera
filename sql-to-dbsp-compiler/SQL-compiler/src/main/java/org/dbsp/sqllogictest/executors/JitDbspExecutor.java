package org.dbsp.sqllogictest.executors;

import com.fasterxml.jackson.databind.JsonNode;
import net.hydromatic.sqllogictest.ISqlTestOperation;
import net.hydromatic.sqllogictest.OptionsParser;
import net.hydromatic.sqllogictest.SltSqlStatement;
import net.hydromatic.sqllogictest.SltTestFile;
import net.hydromatic.sqllogictest.SqlTestQuery;
import net.hydromatic.sqllogictest.SqlTestQueryOutputDescription;
import net.hydromatic.sqllogictest.TestStatistics;
import net.hydromatic.sqllogictest.executors.JdbcExecutor;
import net.hydromatic.sqllogictest.executors.SqlSltTestExecutor;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.backend.ToSqlVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitFileAndSerialization;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitIODescription;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitSerializationKind;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITProgram;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqllogictest.SqlTestPrepareInput;
import org.dbsp.sqllogictest.SqlTestPrepareTables;
import org.dbsp.sqllogictest.SqlTestPrepareViews;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * An executor that uses the DBSP JIT compiler/service to run tests.
 */
public class JitDbspExecutor extends SqlSltTestExecutor {
    protected final JdbcExecutor statementExecutor;
    protected final List<String> tablesCreated;
    final SqlTestPrepareInput inputPreparation;
    final SqlTestPrepareTables tablePreparation;
    final SqlTestPrepareViews viewPreparation;
    protected final CompilerOptions compilerOptions;
    static final String rustDirectory = "../temp/src/";
    static final String projectDirectory = "..";

    public JitDbspExecutor(JdbcExecutor executor,
                           OptionsParser.SuppliedOptions options,
                           CompilerOptions compilerOptions) {
        super(options);
        this.statementExecutor = executor;
        this.tablesCreated = new ArrayList<>();
        this.compilerOptions = compilerOptions;
        this.inputPreparation = new SqlTestPrepareInput();
        this.tablePreparation = new SqlTestPrepareTables();
        this.viewPreparation = new SqlTestPrepareViews();
    }

    Connection getStatementExecutorConnection() {
        return this.statementExecutor.getConnection();
    }

    DBSPZSetLiteral getTableContents(String table) throws SQLException {
        return DbspJdbcExecutor.getTableContents(this.getStatementExecutorConnection(), table);
    }

    void createTables(DBSPCompiler compiler) {
        for (SltSqlStatement statement : this.tablePreparation.statements) {
            String stat = statement.statement;
            compiler.compileStatement(stat, stat);
        }
    }

    @Nullable
    String rewriteCreateTable(String command) throws SQLException {
        Matcher m = DbspJdbcExecutor.PAT_CREATE.matcher(command);
        if (!m.find())
            return null;
        String tableName = m.group(1);
        this.tablesCreated.add(tableName);
        return DbspJdbcExecutor.generateCreateStatement(this.getStatementExecutorConnection(), tableName);
    }

    public boolean statement(SltSqlStatement statement) throws SQLException {
        this.statementExecutor.statement(statement);
        String command = statement.statement.toLowerCase();
        this.options.message("Executing " + command + "\n", 2);

        if (command.startsWith("create index"))
            return true;
        if (command.startsWith("create distinct index"))
            return false;
        if (command.contains("create table") || command.contains("drop table")) {
            String create = this.rewriteCreateTable(command);
            if (create != null) {
                statement = new SltSqlStatement(create, statement.shouldPass);
                this.tablePreparation.add(statement);
            } else {
                Matcher m = DbspJdbcExecutor.PAT_DROP.matcher(command);
                String tableName = m.group(1);
                this.tablesCreated.remove(tableName);
            }
        } else if (command.contains("create view")) {
            this.viewPreparation.add(statement);
        } else if (command.contains("drop view")) {
            this.viewPreparation.remove(statement);
        } else {
            this.inputPreparation.add(statement);
        }
        return true;
    }

    public DBSPExecutor.TableValue[] getInputSets() throws SQLException {
        DBSPExecutor.TableValue[] result = new DBSPExecutor.TableValue[this.tablesCreated.size()];
        int i = 0;
        for (String table: this.tablesCreated) {
            DBSPZSetLiteral lit = this.getTableContents(table);
            result[i++] = new DBSPExecutor.TableValue(table, lit);
        }
        return result;
    }

    File fileFromName(String name) {
        return new File(Paths.get(rustDirectory, name).toUri());
    }

    static DBSPExecutor.TableValue[] previousValues = new DBSPExecutor.TableValue[0];

    boolean query(SqlTestQuery query, TestStatistics statistics, int queryNo)
            throws IOException, InterruptedException, NoSuchAlgorithmException, SQLException {
        if (this.buggyOperations.contains(query.getQuery())
                || this.options.doNotExecute) {
            statistics.incIgnored();
            options.message("Skipping " + query.getQuery(), 2);
            return false;
        }

        DBSPCompiler compiler = new DBSPCompiler(this.compilerOptions);
        this.createTables(compiler);
        String dbspQuery = query.getQuery();
        if (!dbspQuery.toLowerCase().contains("create view"))
            dbspQuery = "CREATE VIEW V AS (" + dbspQuery + ")";
        this.options.message("Query " + queryNo + ":\n"
                + dbspQuery + "\n", 2);
        compiler.generateOutputForNextView(false);
        for (SltSqlStatement view: viewPreparation.definitions()) {
            compiler.compileStatement(view.statement, view.statement);
            compiler.throwIfErrorsOccurred();
        }
        compiler.generateOutputForNextView(true);
        compiler.compileStatement(dbspQuery, "" /* testQuery.getName() */);
        compiler.throwIfErrorsOccurred();
        DBSPExecutor.TableValue[] inputSets = this.getInputSets();

        compiler.optimize();
        DBSPCircuit circuit = compiler.getFinalCircuit("circuit");
        // Serialize circuit as JSON for the JIT executor
        JITProgram program = ToJitVisitor.circuitToJIT(compiler, circuit);
        String json = program.asJson().toPrettyString();

        List<File> toDelete = new ArrayList<>();
        File programFile = this.fileFromName("program.json");
        toDelete.add(programFile);
        Utilities.writeFile(programFile.toPath(), json);
        File asm = this.fileFromName("program.asm");
        toDelete.add(asm);
        Utilities.writeFile(asm.toPath(), program.toString());

        // Prepare input files for the JIT runtime
        boolean sameFiles = Arrays.equals(previousValues, inputSets);
        List<JitFileAndSerialization> inputFiles = new ArrayList<>();
        int index = 0;
        for (DBSPExecutor.TableValue inputData : inputSets) {
            File input = this.fileFromName("input" + index++ + ".csv");
            if (!sameFiles) {
                ToCsvVisitor.toCsv(compiler, input, new DBSPZSetLiteral(
                        compiler.getWeightTypeImplementation(), inputData.contents.data));
            }
            inputFiles.add(new JitFileAndSerialization(
                    input.getAbsolutePath(),
                    JitSerializationKind.Csv));
        }
        previousValues = inputSets;
        List<JitIODescription> inputDescriptions = compiler.getInputDescriptions(inputFiles);

        // Allocate output files
        List<JitFileAndSerialization> outputFiles = new ArrayList<>();
        if (circuit.getOutputCount() != 1)
            throw new RuntimeException("Expected a single output");
        File output = this.fileFromName("output.json");
        toDelete.add(output);
        outputFiles.add(new JitFileAndSerialization(
                output.getAbsolutePath(),
                JitSerializationKind.Json));
        List<JitIODescription> outputDescriptions = compiler.getOutputDescriptions(outputFiles);

        // Invoke the JIT runtime with the program and the configuration file describing inputs and outputs
        JsonNode jitInputDescription = compiler.createJitRuntimeConfig(inputDescriptions, outputDescriptions);
        String s = jitInputDescription.toPrettyString();
        File configFile = this.fileFromName("config.json");
        toDelete.add(configFile);
        Utilities.writeFile(configFile.toPath(), s);
        Utilities.runJIT(projectDirectory, programFile.getAbsolutePath(), configFile.getAbsolutePath());

        // Validate outputs and delete them
        DBSPType outputType = circuit.getOutputType(0);
        JitIODescription outFile = outputDescriptions.get(0);
        DBSPZSetLiteral.Contents actual = outFile.parse(outputType.to(DBSPTypeZSet.class).getElementType());

        boolean result = this.validateOutput(query, queryNo, actual, query.outputDescription, statistics);
        if (!result) {
            for (File file : toDelete) {
                // This point won't be reached if the program fails with an exception
                // or validation fails with -x flag
                boolean success = file.delete();
                if (!success)
                    System.err.println("Failed to delete " + file);
            }
        }
        return result;
    }

    // These should be reused from JdbcExecutor, but are not public there.
    /**
     * A row produced by a query execution.
     */
    static class Row {
        /**
         * In SLT all data received from the database is converted to strings.
         */
        public final List<String> values;
        Row() {
            this.values = new ArrayList<>();
        }
        void add(String v) {
            this.values.add(v);
        }
        @Override public String toString() {
            return String.join(System.lineSeparator(), this.values);
        }
    }

    static class RowComparator implements Comparator<Row> {
        @Override public int compare(Row o1, Row o2) {
            if (o1.values.size() != o2.values.size()) {
                throw new RuntimeException("Comparing rows of different lengths");
            }
            for (int i = 0; i < o1.values.size(); i++) {
                int r = o1.values.get(i).compareTo(o2.values.get(i));
                if (r != 0) {
                    return r;
                }
            }
            return 0;
        }
    }

    /**
     * A set of rows produced as a result of a query execution.
     */
    static class Rows {
        List<Row> allRows;

        Rows() {
            this.allRows = new ArrayList<>();
        }

        void add(Row row) {
            this.allRows.add(row);
        }

        @Override public String toString() {
            return String.join(System.lineSeparator(),
                    Linq.map(this.allRows, Row::toString));
        }

        /**
         * @return Number of rows.
         */
        public int size() {
            return this.allRows.size();
        }

        /**
         * Sort the rows using the specified sort order.
         */
        public void sort(SqlTestQueryOutputDescription.SortOrder order) {
            switch (order) {
                case NONE:
                    break;
                case ROW:
                    this.allRows.sort(new RowComparator());
                    break;
                case VALUE:
                    this.allRows = net.hydromatic.sqllogictest.util.Utilities.flatMap(this.allRows,
                            r -> Linq.map(r.values,
                                    r0 -> {
                                        Row res = new Row();
                                        res.add(r0);
                                        return res;
                                    }));
                    this.allRows.sort(new RowComparator());
                    break;
            }
        }
    }

    static final IErrorReporter errorReporter = new StderrErrorReporter();

    /**
     * Validate output.  Return 'true' if we need to stop executing.
     * Reports errors on validation failures.
     */
    boolean validateOutput(SqlTestQuery query, int queryNo,
                           DBSPZSetLiteral.Contents actual,
                           SqlTestQueryOutputDescription description,
                           TestStatistics statistics) throws NoSuchAlgorithmException {
        StringBuilder builder = new StringBuilder();
        ToSqlVisitor visitor = new ToSqlVisitor(errorReporter, builder);
        Rows rows = new Rows();
        for (Map.Entry<DBSPExpression, Long> entry: actual.data.entrySet()) {
            if (entry.getValue() <= 0)
                return statistics.addFailure(
                        new TestStatistics.FailedTestDescription(query,
                                "Produced negative result", entry.toString(), null));
            for (long i = 0; i < entry.getValue(); i++) {
                DBSPTupleExpression tuple = entry.getKey().to(DBSPTupleExpression.class);
                Row row = new Row();
                for (int j = 0; j < tuple.size(); j++) {
                    DBSPExpression expr = tuple.get(j);
                    // clear builder
                    builder.setLength(0);
                    expr.accept(visitor);
                    row.add(builder.toString());
                }
                rows.add(row);
            }
        }
        if (description.getValueCount()
                != rows.size() * Objects.requireNonNull(description.columnTypes).length()) {
            return statistics.addFailure(
                    new TestStatistics.FailedTestDescription(query,
                            "Expected " + description.getValueCount() + " values, got "
                                    + rows.size() * description.columnTypes.length(),
                            "",
                            null));
        }
        rows.sort(description.getOrder());
        if (description.getQueryResults() != null) {
            String r = rows.toString();
            String q = String.join(System.lineSeparator(),
                    description.getQueryResults());
            if (!r.equals(q)) {
                return statistics.addFailure(
                        new TestStatistics.FailedTestDescription(query,
                                "#" + queryNo + " Output differs from expected value",
                                "computed" + System.lineSeparator()
                                        + r + System.lineSeparator()
                                        + "Expected:" + System.lineSeparator()
                                        + q + System.lineSeparator(),
                                null));
            }
        }
        if (description.hash != null) {
            // MD5 is considered insecure, but we have no choice because this is
            // the algorithm used to compute the checksums by SLT.
            MessageDigest md = MessageDigest.getInstance("MD5");
            String repr = rows + System.lineSeparator();
            md.update(repr.getBytes(StandardCharsets.UTF_8));
            byte[] digest = md.digest();
            String hash = net.hydromatic.sqllogictest.util.Utilities.toHex(digest);
            if (!description.hash.equals(hash)) {
                return statistics.addFailure(
                        new TestStatistics.FailedTestDescription(query,
                                "Hash of data does not match expected value",
                                "expected:" + description.hash + " "
                                        + "computed: " + hash + System.lineSeparator(), null));
            }
        }
        return false;
    }

    @Override
    public TestStatistics execute(SltTestFile testFile, OptionsParser.SuppliedOptions options) throws SQLException, NoSuchAlgorithmException {
        this.statementExecutor.establishConnection();
        this.statementExecutor.dropAllViews();
        this.statementExecutor.dropAllTables();
        this.startTest();
        TestStatistics result = new TestStatistics(
                options.stopAtFirstError, options.verbosity);
        result.incFiles();
        int queryNo = 0;
        int skip = 758;  // used only for debugging
        for (ISqlTestOperation operation : testFile.fileContents) {
            SltSqlStatement stat = operation.as(SltSqlStatement.class);
            if (stat != null) {
                try {
                    this.statement(stat);
                    if (!stat.shouldPass) {
                        options.err.println("Statement should have failed: " + operation);
                    }
                } catch (SQLException ex) {
                    // errors in statements cannot be recovered.
                    if (stat.shouldPass) {
                        // shouldPass should always be true, otherwise
                        // the exception should not be thrown.
                        options.err.println("Error '" + ex.getMessage()
                                + "' in SQL statement " + operation);
                        result.incFilesNotParsed();
                        return result;
                    }
                }
            } else {
                SqlTestQuery query = operation.to(options.err, SqlTestQuery.class);
                boolean stop = false;
                try {
                    if (queryNo >= skip) {
                        stop = this.query(query, result, queryNo);
                    }
                    queryNo++;
                } catch (Throwable ex) {
                    // Need to catch Throwable to handle assertion failures too
                    options.message("Exception during query: " + query.getQuery() + queryNo +
                            ": " + ex.getMessage(), 1);
                    stop = result.addFailure(
                            new TestStatistics.FailedTestDescription(query,
                                    null, "", ex));
                }
                if (stop) {
                    break;
                }
            }
        }
        this.statementExecutor.dropAllViews();
        this.statementExecutor.dropAllTables();
        this.getStatementExecutorConnection().close();
        options.message(this.elapsedTime(queryNo), 1);
        return result;
    }

    public static void register(OptionsParser parser) {
        parser.registerExecutor("jit", () -> {
            OptionsParser.SuppliedOptions options = parser.getOptions();
            try {
                JdbcExecutor inner = Objects.requireNonNull(options.getExecutorByName("hsql"))
                        .as(JdbcExecutor.class);
                CompilerOptions compilerOptions = new CompilerOptions();
                compilerOptions.ioOptions.jit = true;
                compilerOptions.optimizerOptions.throwOnError = options.stopAtFirstError;
                compilerOptions.ioOptions.lenient = true;
                JitDbspExecutor result = new JitDbspExecutor(
                        Objects.requireNonNull(inner), options, compilerOptions);
                Set<String> bugs = options.readBugsFile();
                result.avoid(bugs);
                return result;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}

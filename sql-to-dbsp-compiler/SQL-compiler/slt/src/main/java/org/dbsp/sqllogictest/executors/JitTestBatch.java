package org.dbsp.sqllogictest.executors;

import com.fasterxml.jackson.databind.JsonNode;
import net.hydromatic.sqllogictest.*;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.ToCsvVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitFileAndSerialization;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitIODescription;
import org.dbsp.sqlCompiler.compiler.backend.jit.JitSerializationKind;
import org.dbsp.sqlCompiler.compiler.backend.jit.ToJitVisitor;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITProgram;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.*;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * A batch of tests for the JitDbspExecutor.
 */
public class JitTestBatch extends TestBatch {
    final DBSPCompiler compiler;
    final String jitExecutableDirectory;

    public JitTestBatch(CompilerOptions compilerOptions, OptionsParser.SuppliedOptions options,
                        String filesDirectory, String jitExecutableDirectory, int firstQueryNo) {
        super(options, filesDirectory, firstQueryNo);
        this.jitExecutableDirectory = jitExecutableDirectory;
        this.compiler = new DBSPCompiler(compilerOptions);
    }

    JitTestBatch nextBatch() {
        return new JitTestBatch(this.compiler.options, this.options,
                this.filesDirectory, this.jitExecutableDirectory,this.firstQueryNo + this.queries.size());
    }

    @Override
    public <T extends SltSqlStatement> void prepareInputs(Iterable<T> inputAndViewPreparation) {
        this.compiler.generateOutputForNextView(false);
        for (T statement : inputAndViewPreparation) {
            String stat = statement.statement;
            this.compiler.compileStatement(stat, stat);
            this.compiler.throwIfErrorsOccurred();
        }
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

    Row getValue(DBSPTupleExpression rs, String columnTypes) {
        Row row = new Row();
        for(int i = 0; i < columnTypes.length(); ++i) {
            char c = columnTypes.charAt(i);
            DBSPLiteral value = rs.get(i).to(DBSPLiteral.class);
            if (value.isNull) {
                row.add("NULL");
                continue;
            }
            switch (c) {
                case 'I': {
                    long integer;
                    switch (value.getType().code) {
                        case BOOL:
                            integer = Objects.requireNonNull(value.to(DBSPBoolLiteral.class).value) ? 1 : 0;
                            break;
                        case DECIMAL:
                            integer = Objects.requireNonNull(value.to(DBSPDecimalLiteral.class).value).longValue();
                            break;
                        case DOUBLE:
                            integer = Objects.requireNonNull(value.to(DBSPDoubleLiteral.class).value).longValue();
                            break;
                        case FLOAT:
                            integer = Objects.requireNonNull(value.to(DBSPFloatLiteral.class).value).longValue();
                            break;
                        case INT32:
                            integer = Objects.requireNonNull(value.to(DBSPI32Literal.class).value);
                            break;
                        case INT64:
                            integer = Objects.requireNonNull(value.to(DBSPI64Literal.class).value);
                            break;
                        case STRING:
                            try {
                                integer = Long.parseLong(Objects.requireNonNull(value.to(DBSPStringLiteral.class).value));
                            } catch (NumberFormatException ex) {
                                integer = 0;
                            }
                            break;
                        default:
                            integer = 0;
                            break;
                    }
                    row.add(String.format("%d", integer));
                    break;
                }
                case 'R': {
                    double d;
                    switch (value.getType().code) {
                        case BOOL:
                            d = Objects.requireNonNull(value.to(DBSPBoolLiteral.class).value) ? 1 : 0;
                            break;
                        case DECIMAL:
                            d = Objects.requireNonNull(value.to(DBSPDecimalLiteral.class).value).doubleValue();
                            break;
                        case DOUBLE:
                            d = Objects.requireNonNull(value.to(DBSPDoubleLiteral.class).value);
                            break;
                        case FLOAT:
                            d = Objects.requireNonNull(value.to(DBSPFloatLiteral.class).value);
                            break;
                        case INT32:
                            d = Objects.requireNonNull(value.to(DBSPI32Literal.class).value);
                            break;
                        case INT64:
                            d = Objects.requireNonNull(value.to(DBSPI64Literal.class).value);
                            break;
                        case STRING:
                            try {
                                d = Double.parseDouble(Objects.requireNonNull(value.to(DBSPStringLiteral.class).value));
                            } catch (NumberFormatException ex) {
                                d = 0;
                            }
                            break;
                        default:
                            d = 0;
                            break;
                    }
                    row.add(String.format("%.3f", d));
                    break;
                }
                case 'T': {
                    String s;
                    switch (value.getType().code) {
                        case BOOL:
                            s = Objects.requireNonNull(value.to(DBSPBoolLiteral.class).value) ? "1" : "0";
                            break;
                        case DECIMAL:
                            s = Objects.requireNonNull(value.to(DBSPDecimalLiteral.class).value).toString();
                            break;
                        case DOUBLE:
                            s = Objects.requireNonNull(value.to(DBSPDoubleLiteral.class).value).toString();
                            break;
                        case FLOAT:
                            s = Objects.requireNonNull(value.to(DBSPFloatLiteral.class).value).toString();
                            break;
                        case INT32:
                            s = Objects.requireNonNull(value.to(DBSPI32Literal.class).value).toString();
                            break;
                        case INT64:
                            s = Objects.requireNonNull(value.to(DBSPI64Literal.class).value).toString();
                            break;
                        case STRING:
                            s = Objects.requireNonNull(value.to(DBSPStringLiteral.class).value);
                            break;
                        default:
                            throw new RuntimeException("Unexpected type " + value);
                    }
                    StringBuilder result = new StringBuilder();

                    for (int j = 0; j < s.length(); ++j) {
                        char sc = s.charAt(j);
                        if (sc < ' ' || sc > '~') {
                            sc = '@';
                        }

                        result.append(sc);
                    }

                    row.add(result.toString());
                    break;
                }
                default:
                    throw new RuntimeException("Unexpected column type " + c);
            }
        }
        return row;
    }

    /**
     * Validate output.  Return 'true' if we need to stop executing.
     * Reports errors on validation failures.
     */
    boolean validateOutput(SqlTestQuery query, int queryNo,
                           DBSPZSetLiteral.Contents actual,
                           SqlTestQueryOutputDescription description,
                           TestStatistics statistics) throws NoSuchAlgorithmException {
        Rows rows = new Rows();
        for (Map.Entry<DBSPExpression, Long> entry: actual.data.entrySet()) {
            if (entry.getValue() <= 0)
                return statistics.addFailure(
                        new TestStatistics.FailedTestDescription(query,
                                "#" + queryNo + " produced negative result", entry.toString(), null));
            for (long i = 0; i < entry.getValue(); i++) {
                DBSPTupleExpression tuple = entry.getKey().to(DBSPTupleExpression.class);
                Row row = this.getValue(tuple, Objects.requireNonNull(description.columnTypes));
                rows.add(row);
            }
        }
        if (description.getValueCount()
                != rows.size() * Objects.requireNonNull(description.columnTypes).length()) {
            return statistics.addFailure(
                    new TestStatistics.FailedTestDescription(query,
                            "#" + queryNo + " expected " + description.getValueCount() + " values, got "
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

    boolean mustIgnore(String query) {
        return query.toLowerCase().contains("order by");
    }

    @Override
    boolean run(TestStatistics statistics) throws NoSuchAlgorithmException {
        try {
            // Create the views
            this.compiler.generateOutputForNextView(true);

            for (int i = 0; i < this.queries.size(); i++) {
                SqlTestQuery query = this.queries.get(i);
                String dbspQuery = query.getQuery();
                if (this.mustIgnore(dbspQuery)) {
                    statistics.incIgnored(); // Can't do these yet.
                    continue;
                }
                String viewName = "V" + (i + this.firstQueryNo);
                if (!dbspQuery.toLowerCase().contains("create view"))
                    dbspQuery = "CREATE VIEW " + viewName + " AS (" + dbspQuery + ")";
                this.options.message("Query " + (this.firstQueryNo + i) + ":\n"
                        + dbspQuery + "\n", 2);
                this.compiler.compileStatement(dbspQuery, "");
                this.compiler.throwIfErrorsOccurred();
            }

            this.compiler.optimize();

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

            List<JitFileAndSerialization> inputFiles = new ArrayList<>();
            int index = 0;
            for (DBSPExecutor.TableValue inputData : this.inputContents) {
                File input = this.fileFromName("input" + index++ + ".csv");
                ToCsvVisitor.toCsv(compiler, input, new DBSPZSetLiteral(
                        compiler.getWeightTypeImplementation(), inputData.contents.data));
                inputFiles.add(new JitFileAndSerialization(
                        input.getAbsolutePath(),
                        JitSerializationKind.Csv));
            }
            List<JitIODescription> inputDescriptions = compiler.getInputDescriptions(inputFiles);

            List<JitFileAndSerialization> outputFiles = new ArrayList<>();
            for (int i = 0; i < this.queries.size(); i++) {
                if (this.mustIgnore(this.queries.get(i).getQuery()))
                    continue;
                File output = this.fileFromName("output" + i + ".json");
                toDelete.add(output);
                outputFiles.add(new JitFileAndSerialization(
                        output.getAbsolutePath(),
                        JitSerializationKind.Json));
            }
            List<JitIODescription> outputDescriptions = compiler.getOutputDescriptions(outputFiles);

            // Invoke the JIT runtime with the program and the configuration file describing inputs and outputs
            JsonNode jitInputDescription = compiler.createJitRuntimeConfig(inputDescriptions, outputDescriptions);
            String s = jitInputDescription.toPrettyString();
            File configFile = this.fileFromName("config.json");
            toDelete.add(configFile);
            Utilities.writeFile(configFile.toPath(), s);
            Utilities.runJIT(this.jitExecutableDirectory, programFile.getAbsolutePath(), configFile.getAbsolutePath());

            boolean failed = false;
            for (int i = 0; i < this.queries.size(); i++) {
                SqlTestQuery query = this.queries.get(i);
                if (this.mustIgnore(query.getQuery()))
                    continue;
                DBSPType outputType = circuit.getOutputType(i);
                JitIODescription outFile = outputDescriptions.get(i);
                DBSPZSetLiteral.Contents actual = outFile.parse(outputType.to(DBSPTypeZSet.class).getElementType());
                failed = this.validateOutput(query, this.firstQueryNo + i,
                        actual, query.outputDescription, statistics);
                if (failed)
                    break;
            }

            if (!failed) {
                for (File file : toDelete) {
                    // This point won't be reached if the program fails with an exception
                    // or validation fails with -x flag
                    boolean success = file.delete();
                    if (!success)
                        System.err.println("Failed to delete " + file);
                }
            }
            return failed;
        } catch (InterruptedException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}

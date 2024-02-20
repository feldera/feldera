package org.dbsp.sqlCompiler.compiler.sql;

import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.TimeString;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.TestUtil;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.Change;
import org.dbsp.sqlCompiler.compiler.sql.simple.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBinaryLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDateLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDecimalLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMillisLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPIntervalMonthsLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPRealLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimeLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPVecLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBinary;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDate;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDouble;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMillisInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeMonthsInterval;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeReal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTime;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
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

    // Calcite is not very flexible regarding timestamp formats
    static final SimpleDateFormat[] TIMESTAMP_INPUT_FORMAT = {
            new SimpleDateFormat("EEE MMM d HH:mm:ss.SSS yyyy"),
            new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy G"),
            new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    };
    static final SimpleDateFormat TIMESTAMP_OUTPUT_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

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

    /**
     * Convert a timestamp from a format like Sat Feb 16 17:32:01 1996 to
     * a format like 1996-02-16 17:32:01
     */
    public static DBSPExpression convertTimestamp(@Nullable String timestamp, boolean mayBeNull) {
        if (timestamp == null)
            return DBSPLiteral.none(new DBSPTypeTimestamp(CalciteObject.EMPTY, true));
        for (SimpleDateFormat input: TIMESTAMP_INPUT_FORMAT) {
            String out;
            try {
                // Calcite problems: does not support negative years, or fractional seconds ending in 0
                Date zero = new SimpleDateFormat("yyyy-MM-dd").parse("0000-01-01");
                Date converted = input.parse(timestamp);
                out = TIMESTAMP_OUTPUT_FORMAT.format(converted);
                if (converted.before(zero))
                    out = "-" + out;
            } catch (ParseException ignored) {
                continue;
            }
            return new DBSPTimestampLiteral(out, mayBeNull);
        }
        throw new RuntimeException("Could not parse " + timestamp);
    }

    static final SimpleDateFormat DATE_INPUT_FORMAT = new SimpleDateFormat("MM-dd-yyyy");
    static final SimpleDateFormat DATE_OUTPUT_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Convert a date from the MM-DD-YYYY format (which is used in the Postgres output)
     * to a DBSPLiteral.
     */
    static DBSPExpression parseDate(@Nullable String date, boolean mayBeNull) {
        if (date == null || date.isEmpty() || date.equalsIgnoreCase("null"))
            return DBSPLiteral.none(new DBSPTypeDate(CalciteObject.EMPTY, mayBeNull));
        try {
            if (date.length() != 10)
                throw new RuntimeException("Unexpected date " + date);
            SimpleDateFormat inputFormat;
            if (date.charAt(2) == '-' && date.charAt(5) == '-')
                inputFormat = DATE_INPUT_FORMAT;
            else if (date.charAt(4) == '-' && date.charAt(7) == '-')
                inputFormat = DATE_OUTPUT_FORMAT;
            else
                throw new RuntimeException("Unexpected date " + date);
            Date converted = inputFormat.parse(date);
            String out = DATE_OUTPUT_FORMAT.format(converted);
            return new DBSPDateLiteral(out, mayBeNull);
        } catch (ParseException ex) {
            throw new RuntimeException("Could not parse " + date);
        }
    }

    static DBSPExpression parseTime(@Nullable String time, boolean mayBeNull) {
        if (time == null || time.isEmpty() || time.equalsIgnoreCase("null"))
            return DBSPLiteral.none(new DBSPTypeTime(CalciteObject.EMPTY, mayBeNull));
        return new DBSPTimeLiteral(CalciteObject.EMPTY, new DBSPTypeTime(CalciteObject.EMPTY, mayBeNull), new TimeString(time));
    }

    static final Pattern YEAR = Pattern.compile("^(\\d+) years?(.*)");
    static final Pattern MONTHS = Pattern.compile("^\\s*(\\d+) months?(.*)");

    static int longIntervalToMonths(String interval) {
        String orig = interval;

        int result = 0;
        if (interval.equals("0")) {
            interval = "";
        } else {
            Matcher m = YEAR.matcher(interval);
            if (m.matches()) {
                int months = Integer.parseInt(m.group(1));
                result += months * 12;
                interval = m.group(2);
            }

            m = MONTHS.matcher(interval);
            if (m.matches()) {
                int days = Integer.parseInt(m.group(1));
                result += days;
                interval = m.group(2);
            }

            m = AGO.matcher(interval);
            if (m.matches()) {
                interval = m.group(1);
                result = -result;
            }
        }
        //System.out.println(orig + "->" + result + ": " + interval);
        if (!interval.isEmpty())
            throw new RuntimeException("Could not parse interval " + orig);
        return result;
    }

    static final Pattern MINUS = Pattern.compile("^-(.*)");
    static final Pattern DAYS = Pattern.compile("^(\\d+) days?(.*)");
    static final Pattern HOURS = Pattern.compile("^\\s*(\\d+) hours?(.*)");
    static final Pattern MINUTES = Pattern.compile("\\s*(\\d+) mins?(.*)");
    static final Pattern SECONDS = Pattern.compile("\\s*(\\d+)([.](\\d+))? secs?(.*)");
    static final Pattern HMS = Pattern.compile("\\s*([0-9][0-9]:[0-9][0-9]:[0-9][0-9])(\\.[0-9]*)?(.*)");
    static final Pattern AGO = Pattern.compile("\\s*ago(.*)");

    public static long shortIntervalToMilliseconds(String interval) {
        String orig = interval;
        boolean negate = false;

        long result = 0;
        if (interval.equals("0")) {
            interval = "";
        } else {
            Matcher m = MINUS.matcher(interval);
            if (m.matches()) {
                negate = true;
                interval = m.group(1);
            }

            m = DAYS.matcher(interval);
            if (m.matches()) {
                int d = Integer.parseInt(m.group(1));
                result += (long) d * 86_400_000;
                interval = m.group(2);
            }

            m = HMS.matcher(interval);
            if (m.matches()) {
                String timeString = m.group(1);
                if (m.group(2) != null)
                    timeString += m.group(2);
                TimeString time = new TimeString(timeString);
                result += time.getMillisOfDay();
                interval = m.group(3);
            } else {
                m = HOURS.matcher(interval);
                if (m.matches()) {
                    long h = Integer.parseInt(m.group(1));
                    result += h * 3600_000;
                    interval = m.group(2);
                }

                m = MINUTES.matcher(interval);
                if (m.matches()) {
                    long mm = Integer.parseInt(m.group(1));
                    result += mm * 60_000;
                    interval = m.group(2);
                }

                m = SECONDS.matcher(interval);
                if (m.matches()) {
                    long s = Integer.parseInt(m.group(1));
                    result += s * 1000;
                    interval = m.group(4);
                }
            }

            m = AGO.matcher(interval);
            if (m.matches()) {
                interval = m.group(1);
                negate = !negate;
            }

            if (negate)
                result = -result;
        }
        //System.out.println(orig + "->" + result + ": " + interval);
        if (!interval.isEmpty())
            throw new RuntimeException("Could not parse interval " + orig);
        return result;
    }

    DBSPExpression parseValue(DBSPType fieldType, String data) {
        String trimmed = data.trim();
        DBSPExpression result;
        if (!fieldType.is(DBSPTypeString.class) &&
                (trimmed.isEmpty() ||
                        trimmed.equalsIgnoreCase("null"))) {
            if (!fieldType.mayBeNull)
                throw new RuntimeException("Null value in non-nullable column " + fieldType);
            result = fieldType.nullValue();
        } else if (fieldType.is(DBSPTypeDouble.class)) {
            double value = Double.parseDouble(trimmed);
            result = new DBSPDoubleLiteral(value, fieldType.mayBeNull);
        } else if (fieldType.is(DBSPTypeReal.class)) {
            float value = Float.parseFloat(trimmed);
            result = new DBSPRealLiteral(value, fieldType.mayBeNull);
        } else if (fieldType.is(DBSPTypeDecimal.class)) {
            BigDecimal value = new BigDecimal(trimmed);
            result = new DBSPDecimalLiteral(fieldType, value);
        } else if (fieldType.is(DBSPTypeTimestamp.class)) {
            result = convertTimestamp(trimmed, fieldType.mayBeNull);
        } else if (fieldType.is(DBSPTypeDate.class)) {
            result = parseDate(trimmed, fieldType.mayBeNull);
        } else if (fieldType.is(DBSPTypeTime.class)) {
            result = parseTime(trimmed, fieldType.mayBeNull);
        } else if (fieldType.is(DBSPTypeInteger.class)) {
            DBSPTypeInteger intType = fieldType.to(DBSPTypeInteger.class);
            result = switch (intType.getWidth()) {
                case 8 -> new DBSPI8Literal(Byte.parseByte(trimmed), fieldType.mayBeNull);
                case 16 -> new DBSPI16Literal(Short.parseShort(trimmed), fieldType.mayBeNull);
                case 32 -> new DBSPI32Literal(Integer.parseInt(trimmed), fieldType.mayBeNull);
                case 64 -> new DBSPI64Literal(Long.parseLong(trimmed), fieldType.mayBeNull);
                default -> throw new UnimplementedException(intType);
            };
        } else if (fieldType.is(DBSPTypeMillisInterval.class)) {
            long value = shortIntervalToMilliseconds(trimmed);
            result = new DBSPIntervalMillisLiteral(value, fieldType.mayBeNull);
        } else if (fieldType.is(DBSPTypeMonthsInterval.class)) {
            int months = longIntervalToMonths(trimmed);
            result = new DBSPIntervalMonthsLiteral(months);
        } else if (fieldType.is(DBSPTypeString.class)) {
            // If there is no space in front of the string, we expect a NULL.
            // This is how we distinguish empty strings from nulls.
            if (!data.startsWith(" ")) {
                if (data.equals("NULL"))
                    result = DBSPLiteral.none(fieldType);
                else
                    throw new RuntimeException("Expected NULL or a space: " +
                            Utilities.singleQuote(data));
            } else {
                data = data.substring(1);
                result = new DBSPStringLiteral(CalciteObject.EMPTY, fieldType, data, StandardCharsets.UTF_8);
            }
        } else if (fieldType.is(DBSPTypeBool.class)) {
            boolean isTrue = trimmed.equalsIgnoreCase("t") || trimmed.equalsIgnoreCase("true");
            boolean isFalse = trimmed.equalsIgnoreCase("f") || trimmed.equalsIgnoreCase("false");
            if (!isTrue && !isFalse)
                throw new RuntimeException("Cannot parse boolean value " + Utilities.singleQuote(trimmed));
            result = new DBSPBoolLiteral(CalciteObject.EMPTY, fieldType, isTrue);
        } else if (fieldType.is(DBSPTypeVec.class)) {
            DBSPTypeVec vec = fieldType.to(DBSPTypeVec.class);
            // TODO: this does not handle nested arrays
            if (trimmed.equals("NULL")) {
                result = new DBSPVecLiteral(fieldType, true);
            } else {
                if (!trimmed.startsWith("{") || !trimmed.endsWith("}"))
                    throw new UnimplementedException("Expected array constant to be bracketed: " + trimmed);
                trimmed = trimmed.substring(1, trimmed.length() - 1);
                if (!trimmed.isEmpty()) {
                    // an empty string split still returns an empty string
                    String[] parts = trimmed.split(",");
                    DBSPExpression[] fields;
                    fields = Linq.map(
                            parts, p -> this.parseValue(vec.getElementType(), p), DBSPExpression.class);
                    result = new DBSPVecLiteral(fieldType.mayBeNull, fields);
                } else {
                    // empty vector
                    result = new DBSPVecLiteral(vec.getElementType());
                }
            }
        } else if (fieldType.is(DBSPTypeBinary.class)) {
            if (!data.startsWith(" ")) {
                if (data.equals("NULL"))
                    result = DBSPLiteral.none(fieldType);
                else
                    throw new RuntimeException("Expected NULL or a space: " +
                            Utilities.singleQuote(data));
            } else {
                data = data.trim();
                byte[] bytes = ConversionUtil.toByteArrayFromString(data, 16);
                result = new DBSPBinaryLiteral(CalciteObject.EMPTY, fieldType, bytes);
            }
        } else {
            throw new UnimplementedException(fieldType);
        }
        return result;
    }

    public DBSPTupleExpression parseRow(String line, DBSPTypeTupleBase rowType, String separatorRegex) {
        String[] columns;
        if (rowType.size() > 1) {
            columns = line.split(separatorRegex);
        } else {
            // Do not split; allows handling 1-column outputs that contains |
            columns = new String[1];
            columns[0] = line;
        }
        if (columns.length != rowType.size())
            throw new RuntimeException("Row has " + columns.length +
                    " columns, but expected " + rowType.size() + ": " +
                    Utilities.singleQuote(line));
        DBSPExpression[] values = new DBSPExpression[columns.length];
        for (int i = 0; i < columns.length; i++) {
            DBSPType fieldType = rowType.getFieldType(i);
            values[i] = this.parseValue(fieldType, columns[i]);
        }
        return new DBSPTupleExpression(values);
    }

    public Change parseTable(String table, DBSPType outputType) {
        DBSPTypeZSet zset = outputType.to(DBSPTypeZSet.class);
        DBSPZSetLiteral result = DBSPZSetLiteral.emptyWithElementType(zset.elementType);
        DBSPTypeTuple tuple = zset.elementType.to(DBSPTypeTuple.class);

        // We parse tables in three formats:
        // Postgres
        // t | t | f
        //---+---+---
        // t | t | f
        //
        // MySQL framed
        // +-----------+----------+----------+----------+
        // | JOB       | 10_COUNT | 50_COUNT | 20_COUNT |
        // +-----------+----------+----------+----------+
        // | ANALYST   |        0 |        0 |        2 |
        // | CLERK     |        1 |        0 |        2 |
        // | MANAGER   |        1 |        0 |        1 |
        // | PRESIDENT |        1 |        0 |        0 |
        // | SALESMAN  |        0 |        0 |        0 |
        // +-----------+----------+----------+----------+
        //
        // MySQL tab separated
        // header1\theader2\n
        // value1\tvalue2\n
        boolean mysqlStyle = false;
        String separator = "[|]";

        String[] lines = table.split("\n", -1);
        boolean inHeader = true;
        int horizontalLines = 0;
        for (String line: lines) {
            if (inHeader) {
                if (line.isEmpty())
                    continue;
                if (line.startsWith("+---"))
                    mysqlStyle = true;
                if (line.contains("\t")) {
                    separator = "\t";
                    inHeader = false;
                    continue;
                }
            }
            if (line.contains("---")) {
                horizontalLines++;
                if (mysqlStyle) {
                    if (horizontalLines == 2)
                        inHeader = false;
                } else {
                    inHeader = false;
                }
                continue;
            }
            if (horizontalLines == 3)
                // After table.
                continue;
            if (inHeader)
                continue;
            if (mysqlStyle && line.startsWith("|") && line.endsWith("|"))
                line = line.substring(1, line.length() - 2);
            DBSPExpression row = this.parseRow(line, tuple, separator);
            result.add(row);
        }
        if (inHeader)
            throw new RuntimeException("Could not find end of header for table");
        return new Change(result);
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
        DBSPCircuit circuit = getCircuit(compiler);
        InputOutputChange iochange = new InputOutputChange(
                this.getPreparedInputs(compiler),
                new Change(expected)
        );
        this.addRustTestCase(query, compiler, circuit, iochange.toStream());
    }

    public void compare(String query, String expected, boolean optimize) {
        DBSPCompiler compiler = this.testCompiler(optimize);
        this.prepareInputs(compiler);
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        if (!compiler.options.languageOptions.throwOnError)
            compiler.throwIfErrorsOccurred();
        compiler.optimize();
        DBSPCircuit circuit = getCircuit(compiler);
        if (!compiler.messages.isEmpty())
            System.out.println(compiler.messages);
        DBSPType outputType = circuit.getSingleOutputType();
        Change result = this.parseTable(expected, outputType);
        InputOutputChange streams = new InputOutputChange(
                this.getPreparedInputs(compiler),
                result
        );
        this.addRustTestCase(query, compiler, circuit, streams.toStream());
    }

    /**
     * Test a query followed by the expected output.
     * The query ends at the semicolon.
     * Runs two test cases, one with optimizations and one without.
     * This makes sure that constant queries still exercise the runtime.
     */
    public void q(String queryAndOutput, boolean twoWays) {
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

    /**
     * Run a query that is expected to fail in compilation.
     * @param query             Query to run.
     * @param messageFragment   This fragment should appear in the error message.
     */
    public void shouldFail(String query, String messageFragment) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.options.languageOptions.throwOnError = false;
        this.prepareInputs(compiler);
        compiler.compileStatement("CREATE VIEW VV AS " + query);
        compiler.optimize();
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
        for (String part: parts) {
            Matcher regexMatcher = regex.matcher(part);
            if (regexMatcher.find()) {
                String result = regexMatcher.group(1);
                this.q(result, twoWays);
            } else {
                throw new RuntimeException("Could not understand test: " + part);
            }
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
        compiler.optimize();
        DBSPCircuit circuit = getCircuit(compiler);
        DBSPType outputType = circuit.getSingleOutputType();
        Change result = new Change(new DBSPZSetLiteral(Collections.emptyMap(), outputType));
        InputOutputChange ioChange = new InputOutputChange(
                this.getPreparedInputs(compiler),
                result
        );
        this.addFailingRustTestCase(query, panicMessage, compiler, circuit, ioChange.toStream());
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

package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.TimeString;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
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
import org.dbsp.sqlCompiler.ir.expression.DBSPArrayExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU16Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPU8Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPUuidLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Helper class for tests which is used to parse the expected output from queries */
public class TableParser {
    // Append second fractions
    static DateTimeFormatterBuilder sf(String pattern) {
        return new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .appendLiteral('.')
                .appendFraction(ChronoField.MILLI_OF_SECOND, 1, 3, false);
    }

    // The first few of these are postgres specific
    static final DateTimeFormatter[] TIMESTAMP_INPUT_FORMAT = {
            sf("EEE MMM d HH:mm:ss")
                    .appendPattern(" yyyy")
                    .toFormatter(Locale.ENGLISH),
            DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss yyyy G", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss yyyy", Locale.ENGLISH),
            sf("yyyy-MM-dd HH:mm:ss")
                    .toFormatter(Locale.ENGLISH),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    };

    static final DateTimeFormatter TIMESTAMP_OUTPUT_FORMAT = sf("yyyy-MM-dd HH:mm:ss").toFormatter(Locale.ENGLISH);
    static final DateTimeFormatter DATE_INPUT_FORMAT = DateTimeFormatter.ofPattern("MM-dd-yyyy", Locale.ENGLISH);
    static final DateTimeFormatter DATE_OUTPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
    static final Pattern YEAR = Pattern.compile("^(\\d+) years?(.*)");
    static final Pattern MONTHS = Pattern.compile("^\\s*(\\d+) months?(.*)");
    static final Pattern MINUS = Pattern.compile("^-\\s*(.*)");
    static final Pattern DAYS = Pattern.compile("^(\\d+) days?(.*)");
    static final Pattern HOURS = Pattern.compile("^\\s*(\\d+) hours?(.*)");
    static final Pattern MINUTES = Pattern.compile("\\s*(\\d+) mins?(.*)");
    static final Pattern SECONDS = Pattern.compile("\\s*(\\d+)([.](\\d+))? secs?(.*)");
    static final Pattern HMS = Pattern.compile("\\s*([0-9][0-9]:[0-9][0-9]:[0-9][0-9])(\\.[0-9]*)?(.*)");
    static final Pattern AGO = Pattern.compile("\\s*ago(.*)");
    @Nullable
    static LocalDateTime ZERO = null;

    /** Convert a timestamp from a format like Sat Feb 16 17:32:01 1996 to
     * a format like 1996-02-16 17:32:01 */
    public static DBSPExpression convertTimestamp(@Nullable String timestamp, DBSPType type) {
        if (timestamp == null)
            return DBSPLiteral.none(type);
        try {
            if (ZERO == null)
                ZERO = LocalDateTime.parse("0001-01-01 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        } catch (DateTimeParseException e) {
            throw new RuntimeException(e);
        }
        for (DateTimeFormatter input: TIMESTAMP_INPUT_FORMAT) {
            String out;
            try {
                // Calcite problems: does not support negative years, or fractional seconds ending in 0
                LocalDateTime converted = LocalDateTime.parse(timestamp, input);
                out = TIMESTAMP_OUTPUT_FORMAT.format(converted);
                if (converted.isBefore(ZERO))
                    out = "-" + out;
            } catch (DateTimeParseException ignored) {
                continue;
            }
            return new DBSPTimestampLiteral(out, type.mayBeNull);
        }
        throw new RuntimeException("Could not parse " + timestamp + " as timestamp");
    }

    /** Convert a date from the MM-DD-YYYY format (which is used in the Postgres output)
     * to a DBSPLiteral. */
    static DBSPExpression parseDate(@Nullable String date, DBSPType type) {
        if (date == null || date.isEmpty() || date.equalsIgnoreCase("null"))
            return DBSPLiteral.none(type);
        try {
            if (date.length() != 10)
                throw new RuntimeException("Unexpected date " + date);
            DateTimeFormatter inputFormat;
            if (date.charAt(2) == '-' && date.charAt(5) == '-')
                inputFormat = DATE_INPUT_FORMAT;
            else if (date.charAt(4) == '-' && date.charAt(7) == '-')
                inputFormat = DATE_OUTPUT_FORMAT;
            else
                throw new RuntimeException("Unexpected date " + date);
            LocalDate converted = LocalDate.parse(date, inputFormat);
            String out = DATE_OUTPUT_FORMAT.format(converted);
            return new DBSPDateLiteral(out, type.mayBeNull);
        } catch (DateTimeParseException ex) {
            throw new RuntimeException("Could not parse " + date);
        }
    }

    static DBSPExpression parseTime(@Nullable String time, DBSPType type) {
        if (time == null || time.isEmpty() || time.equalsIgnoreCase("null"))
            return DBSPLiteral.none(type);
        return new DBSPTimeLiteral(CalciteObject.EMPTY, type, new TimeString(time));
    }

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
                    long h = Long.parseLong(m.group(1));
                    result += h * 3600_000;
                    interval = m.group(2);
                }

                m = MINUTES.matcher(interval);
                if (m.matches()) {
                    long mm = Long.parseLong(m.group(1));
                    result += mm * 60_000;
                    interval = m.group(2);
                }

                m = SECONDS.matcher(interval);
                if (m.matches()) {
                    long s = Long.parseLong(m.group(1));
                    result += s * 1000;
                    if (m.group(3) != null) {
                        String msec = m.group(3) + "000";
                        result += Long.parseLong(msec.substring(0, 3));
                    }
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

    static DBSPExpression parseValue(DBSPType fieldType, String data) {
        String trimmed = data.trim();
        DBSPExpression result;
        if ((fieldType.code != DBSPTypeCode.STRING && fieldType.code != DBSPTypeCode.BYTES) &&
                (trimmed.isEmpty() ||
                        trimmed.equalsIgnoreCase("null"))) {
            if (!fieldType.mayBeNull)
                throw new RuntimeException("Null value in non-nullable column " + fieldType);
            result = fieldType.nullValue();
        } else {
            result = switch (fieldType.code) {
                case DOUBLE -> {
                    double value = Double.parseDouble(trimmed);
                    yield new DBSPDoubleLiteral(CalciteObject.EMPTY, fieldType, value);
                }
                case REAL -> {
                    float value = Float.parseFloat(trimmed);
                    yield new DBSPRealLiteral(CalciteObject.EMPTY, fieldType, value);
                }
                case DECIMAL -> {
                    BigDecimal value = new BigDecimal(trimmed);
                    yield new DBSPDecimalLiteral(fieldType, value);
                }
                case TIMESTAMP -> convertTimestamp(trimmed, fieldType);
                case DATE -> parseDate(trimmed, fieldType);
                case TIME -> parseTime(trimmed, fieldType);
                case INT8 -> new DBSPI8Literal(CalciteObject.EMPTY, fieldType, Byte.parseByte(trimmed));
                case INT16 -> new DBSPI16Literal(CalciteObject.EMPTY, fieldType, Short.parseShort(trimmed));
                case INT32 -> new DBSPI32Literal(CalciteObject.EMPTY, fieldType, Integer.parseInt(trimmed));
                case INT64 -> new DBSPI64Literal(CalciteObject.EMPTY, fieldType, Long.parseLong(trimmed));
                case UINT8 -> new DBSPU8Literal(CalciteObject.EMPTY, fieldType, Integer.parseInt(trimmed));
                case UINT16 -> new DBSPU16Literal(CalciteObject.EMPTY, fieldType, Integer.parseInt(trimmed));
                case UINT32 -> new DBSPU32Literal(CalciteObject.EMPTY, fieldType, Long.parseLong(trimmed));
                case UINT64 -> new DBSPU64Literal(CalciteObject.EMPTY, fieldType, new BigInteger(trimmed));
                case INTERVAL_SHORT -> {
                    long value = shortIntervalToMilliseconds(trimmed);
                    yield new DBSPIntervalMillisLiteral(CalciteObject.EMPTY, fieldType, value);
                }
                case INTERVAL_LONG -> {
                    int months = longIntervalToMonths(trimmed);
                    yield new DBSPIntervalMonthsLiteral(CalciteObject.EMPTY, fieldType, months);
                }
                case STRING -> {
                    // If there is no space in front of the string, we expect a NULL.
                    // This is how we distinguish empty strings from nulls.
                    if (!data.startsWith(" ")) {
                        if (data.startsWith("NULL"))
                            yield DBSPLiteral.none(fieldType);
                        else
                            throw new RuntimeException("Expected NULL or a space: " +
                                    Utilities.singleQuote(data));
                    } else {
                        // replace \\n with \n, otherwise we can't represent it
                        data = data.substring(1);
                        data = data.replace("\\n", "\n");
                        yield new DBSPStringLiteral(CalciteObject.EMPTY, fieldType, data, StandardCharsets.UTF_8);
                    }
                }
                case BOOL -> {
                    boolean isTrue = trimmed.equalsIgnoreCase("t") || trimmed.equalsIgnoreCase("true");
                    boolean isFalse = trimmed.equalsIgnoreCase("f") || trimmed.equalsIgnoreCase("false");
                    if (!isTrue && !isFalse)
                        throw new RuntimeException("Cannot parse boolean value " + Utilities.singleQuote(trimmed));
                    yield new DBSPBoolLiteral(CalciteObject.EMPTY, fieldType, isTrue);
                }
                case ARRAY -> {
                    DBSPTypeArray vec = fieldType.to(DBSPTypeArray.class);
                    // TODO: this does not handle nested arrays
                    if (trimmed.equals("NULL")) {
                        yield new DBSPArrayExpression(fieldType, true);
                    } else {
                        if (!trimmed.startsWith("{") || !trimmed.endsWith("}"))
                            throw new UnimplementedException("Expected array constant to be bracketed: " + trimmed);
                        trimmed = trimmed.substring(1, trimmed.length() - 1);
                        if (!trimmed.isEmpty()) {
                            // an empty string split still returns an empty string
                            String[] parts = trimmed.split(",");
                            DBSPExpression[] fields;
                            fields = Linq.map(
                                    parts, p -> parseValue(vec.getElementType(), p), DBSPExpression.class);
                            yield new DBSPArrayExpression(fieldType.mayBeNull, fields);
                        } else {
                            // empty vector
                            yield new DBSPArrayExpression(vec, false);
                        }
                    }
                }
                case BYTES -> {
                    if (!data.startsWith(" ")) {
                        if (data.equals("NULL"))
                            yield DBSPLiteral.none(fieldType);
                        else
                            throw new RuntimeException("Expected NULL or a space: " +
                                    Utilities.singleQuote(data));
                    } else {
                        data = data.trim();
                        byte[] bytes = ConversionUtil.toByteArrayFromString(data, 16);
                        yield new DBSPBinaryLiteral(CalciteObject.EMPTY, fieldType, bytes);
                    }
                }
                case UUID -> {
                    if (!data.startsWith(" ")) {
                        if (data.equals("NULL"))
                            yield DBSPLiteral.none(fieldType);
                        else
                            throw new RuntimeException("Expected NULL or a space: " +
                                    Utilities.singleQuote(data));
                    } else {
                        UUID uuid = UUID.fromString(trimmed);
                        yield new DBSPUuidLiteral(CalciteObject.EMPTY, fieldType, uuid);
                    }
                }
                case TUPLE -> {
                    // TODO: this does not handle nested tuples, or tuples with arrays
                    DBSPTypeTuple tuple = fieldType.to(DBSPTypeTuple.class);
                    if (trimmed.equals("NULL")) {
                        yield tuple.none();
                    } else {
                        if (!trimmed.startsWith("{") || !trimmed.endsWith("}"))
                            throw new UnimplementedException("Expected tuple constant to be bracketed: " + trimmed);
                        trimmed = trimmed.substring(1, trimmed.length() - 1);
                        Utilities.enforce(!trimmed.isEmpty());

                        String[] parts = trimmed.split(",");
                        Utilities.enforce(parts.length == tuple.size(), "Expected " + tuple.size() + " fields for tuple, got " + parts.length);
                        List<DBSPExpression> fields = new ArrayList<>(tuple.size());
                        int index = 0;
                        for (DBSPType ft : tuple.tupFields)
                            fields.add(parseValue(ft, parts[index++]));
                        yield new DBSPTupleExpression(CalciteObject.EMPTY, tuple, fields);
                    }
                }
                default -> throw new UnimplementedException(
                        "Support for parsing fields of type " + fieldType + " not yet implemented", fieldType);
            };
        }
        return result;
    }

    public static DBSPTupleExpression parseRow(String line, DBSPTypeTupleBase rowType, String separatorRegex) {
        String[] columns;
        if (rowType.size() > 1) {
            columns = line.split(separatorRegex, -1);
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
            values[i] = parseValue(fieldType, columns[i]);
        }
        return new DBSPTupleExpression(values);
    }

    /** Parse a change table.  A change table is like a table, but has an extra
     * integer column that contains the weights. */
    public static Change parseChangeTable(String table, DBSPType outputType) {
        List<DBSPType> extraFields =
                Linq.list(outputType.to(DBSPTypeZSet.class).elementType.to(DBSPTypeTuple.class).tupFields);
        extraFields.add(new DBSPTypeInteger(CalciteObject.EMPTY, 64, true, false));
        DBSPType extraOutputType = new DBSPTypeTuple(extraFields);
        Change change = parseTable(table, new DBSPTypeZSet(extraOutputType), -1);
        DBSPZSetExpression[] extracted = Linq.map(change.sets, SqlIoTest::extractWeight, DBSPZSetExpression.class);
        return new Change(extracted);
    }

    public static Change parseTable(String table, DBSPType outputType, int rowCount) {
        DBSPTypeZSet zset = outputType.to(DBSPTypeZSet.class);
        DBSPZSetExpression result = DBSPZSetExpression.emptyWithElementType(zset.elementType);
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
        int rowsFound = 0;
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
                line = line.substring(1, line.length() - 1);
            DBSPExpression row = parseRow(line, tuple, separator);
            result.append(row);
            rowsFound++;
        }
        if (rowCount >= 0 && rowCount != rowsFound)
            throw new RuntimeException("Expected " + rowCount + " rows, found " + rowsFound + ": " + table);
        if (inHeader)
            throw new RuntimeException("Could not find end of header for table " + table);
        return new Change(result);
    }

    public static Change fromResultSet(ResultSet data, DBSPType outputType) throws SQLException {
        ResultSetMetaData metaData = data.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder builder = new StringBuilder();
        // column headers
        for (int i = 0; i < columnCount; i++) {
            if (i > 0)
                builder.append("\t");
            builder.append(metaData.getColumnName(i + 1));
        }
        builder.append("\tweight");
        while (data.next()) {
            builder.append(System.lineSeparator());
            for (int i = 0; i < columnCount; i++) {
                if (i > 0)
                    builder.append("\t");
                Object value = data.getObject(i+1);
                if (value == null)
                    builder.append("NULL");
                else {
                    builder.append(" ").append(value);
                }
            }
            builder.append("\t1");
        }

        return parseChangeTable(builder.toString(), outputType);
    }
}

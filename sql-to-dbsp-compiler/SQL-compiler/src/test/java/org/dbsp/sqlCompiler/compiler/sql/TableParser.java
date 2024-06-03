package org.dbsp.sqlCompiler.compiler.sql;

import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.TimeString;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.Change;
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
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeZSet;
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

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Helper class for tests which is used to parse the expected output from queries */
public class TableParser {
    static final SimpleDateFormat[] TIMESTAMP_INPUT_FORMAT = {
            new SimpleDateFormat("EEE MMM d HH:mm:ss.SSS yyyy"),
            new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy G"),
            new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    };
    static final SimpleDateFormat TIMESTAMP_OUTPUT_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    static final SimpleDateFormat DATE_INPUT_FORMAT = new SimpleDateFormat("MM-dd-yyyy");
    static final SimpleDateFormat DATE_OUTPUT_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    static final Pattern YEAR = Pattern.compile("^(\\d+) years?(.*)");
    static final Pattern MONTHS = Pattern.compile("^\\s*(\\d+) months?(.*)");
    static final Pattern MINUS = Pattern.compile("^-(.*)");
    static final Pattern DAYS = Pattern.compile("^(\\d+) days?(.*)");
    static final Pattern HOURS = Pattern.compile("^\\s*(\\d+) hours?(.*)");
    static final Pattern MINUTES = Pattern.compile("\\s*(\\d+) mins?(.*)");
    static final Pattern SECONDS = Pattern.compile("\\s*(\\d+)([.](\\d+))? secs?(.*)");
    static final Pattern HMS = Pattern.compile("\\s*([0-9][0-9]:[0-9][0-9]:[0-9][0-9])(\\.[0-9]*)?(.*)");
    static final Pattern AGO = Pattern.compile("\\s*ago(.*)");


    /** Convert a timestamp from a format like Sat Feb 16 17:32:01 1996 to
     * a format like 1996-02-16 17:32:01 */
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
        throw new RuntimeException("Could not parse " + timestamp + " as timestamp");
    }

    /** Convert a date from the MM-DD-YYYY format (which is used in the Postgres output)
     * to a DBSPLiteral. */
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
        return new DBSPTimeLiteral(CalciteObject.EMPTY,
                new DBSPTypeTime(CalciteObject.EMPTY, mayBeNull), new TimeString(time));
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

    static DBSPExpression parseValue(DBSPType fieldType, String data) {
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
                            parts, p -> parseValue(vec.getElementType(), p), DBSPExpression.class);
                    result = new DBSPVecLiteral(fieldType.mayBeNull, fields);
                } else {
                    // empty vector
                    result = new DBSPVecLiteral(vec, false);
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
        Change change = parseTable(table, new DBSPTypeZSet(extraOutputType));
        DBSPZSetLiteral[] extracted = Linq.map(change.sets, SqlIoTest::extractWeight, DBSPZSetLiteral.class);
        return new Change(extracted);
    }

    public static Change parseTable(String table, DBSPType outputType) {
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
                line = line.substring(1, line.length() - 1);
            DBSPExpression row = parseRow(line, tuple, separator);
            result.add(row);
        }
        if (inHeader)
            throw new RuntimeException("Could not find end of header for table " + table);
        return new Change(result);
    }
}

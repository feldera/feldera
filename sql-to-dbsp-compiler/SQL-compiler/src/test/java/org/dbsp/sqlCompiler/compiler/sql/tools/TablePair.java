package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** A pair of tables with the same columns: table LATE_name with LATENESS on some columns and
 * table PLAIN_name without; every change goes to both tables or to neither.
 * @param <R>  Type of a row. */
public final class TablePair<R extends Record> {
    /** The prefixes of the names of the two tables. */
    public static final String[] PREFIXES = { "LATE_", "PLAIN_" };
    static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** A column of the tables, read from the record component {@code component}. */
    record Column(RecordComponent component, String sqlType) {
        String name() {
            return this.component.getName();
        }

        /** True if the column can hold NULL in a nullable pair. */
        boolean mayBeNull() {
            return !this.component.getType().isPrimitive();
        }

        /** The value of the column in {@code row}; null for NULL. */
        @Nullable
        Object read(Record row) {
            return TablePair.read(this.component, row);
        }
    }

    public final String name;
    /** The class of the rows of the tables. */
    final Class<R> rowClass;
    /** True if the boxed columns of the tables accept NULL. */
    public final boolean nullable;
    final List<Column> columns = new ArrayList<>();
    /** The columns of the LATE_ table that have LATENESS. */
    public final LatenessSchema<R> schema = new LatenessSchema<>();

    /** @param name         Suffix of the table names: the tables are LATE_name and PLAIN_name.
     *  @param nullable     True if the boxed columns of the tables accept NULL.
     *  @param rowClass     The record whose components are the columns of the tables.
     *  @param lateColumns  The columns with LATENESS in the LATE_ table; each is a component of the record. */
    public TablePair(String name, boolean nullable, Class<R> rowClass, List<ColumnWithLateness<R, ?>> lateColumns) {
        this.name = name;
        this.rowClass = rowClass;
        this.nullable = nullable;
        for (RecordComponent component : rowClass.getRecordComponents()) {
            // The row records are nested in the test classes, which are in another package
            component.getAccessor().setAccessible(true);
            this.columns.add(new Column(component, sqlType(component.getType())));
        }
        for (ColumnWithLateness<R, ?> late : lateColumns) {
            Column column = this.column(late.name);
            Utilities.enforce(column != null, () -> "No column " + late + " in " + rowClass);
            Utilities.enforce(column.sqlType.equals(late.sqlType()),
                    () -> "Column " + late + " with LATENESS is not a " + late.sqlType());
            this.schema.addColumn(late);
        }
    }

    /** The column named {@code name}, or null if there is none. */
    @Nullable
    Column column(String name) {
        for (Column column : this.columns)
            if (column.name().equals(name))
                return column;
        return null;
    }

    /** @param name         Suffix of the table names: the tables are LATE_name and PLAIN_name.
     *  @param nullable     True if the boxed columns of the tables accept NULL.
     *  @param rowClass     The record whose components are the columns of the tables.
     *  @param lateColumns  Names of the columns with LATENESS in the LATE_ table; each is an INT column.
     *  @param lateness     The LATENESS of these columns. */
    public TablePair(String name, boolean nullable, Class<R> rowClass, Set<String> lateColumns, int lateness) {
        this(name, nullable, rowClass, integerColumns(rowClass, lateColumns, lateness));
    }

    /** An INT column with LATENESS {@code lateness} for each component of {@code rowClass} named in
     * {@code names}. */
    static <R extends Record> List<ColumnWithLateness<R, ?>> integerColumns(
            Class<R> rowClass, Set<String> names, int lateness) {
        List<ColumnWithLateness<R, ?>> result = new ArrayList<>();
        for (RecordComponent component : rowClass.getRecordComponents()) {
            if (!names.contains(component.getName()))
                continue;
            component.getAccessor().setAccessible(true);
            result.add(new IntegerColumn<R>(component.getName(), row -> (Integer) read(component, row), lateness));
        }
        Utilities.enforce(result.size() == names.size(), () -> "Not all of " + names + " are columns of " + rowClass);
        return result;
    }

    /** The value of {@code component} in {@code row}; null for NULL. */
    @Nullable
    static Object read(RecordComponent component, Record row) {
        try {
            return component.getAccessor().invoke(row);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** The SQL type of a column for a record component of type {@code type}. */
    static String sqlType(Class<?> type) {
        if (type == int.class || type == Integer.class)
            return "INT";
        if (type == long.class || type == Long.class)
            return "BIGINT";
        if (type == LocalDateTime.class)
            return "TIMESTAMP";
        if (type == LocalDate.class)
            return "DATE";
        throw new UnsupportedOperationException("No SQL type for " + type);
    }

    /** The CREATE TABLE statement for one table of the pair.
     * @param prefix    Prefix of the table name, LATE_ or PLAIN_.
     * @param lateness  True to add LATENESS to the columns in the schema. */
    public String create(String prefix, boolean lateness) {
        List<String> declarations = new ArrayList<>();
        for (Column column : this.columns) {
            String declaration = column.name() + " " + column.sqlType;
            if (!column.mayBeNull() || !this.nullable)
                declaration += " NOT NULL";
            if (lateness)
                declaration += this.schema.lateness(column.name());
            declarations.add(declaration);
        }
        return "CREATE TABLE " + prefix + this.name + " (" + String.join(", ", declarations) + ");\n";
    }

    /** True if the row can be stored in the tables: a NULL needs a nullable pair. */
    boolean fits(R row) {
        for (Column column : this.columns)
            if (column.read(row) == null && !this.nullable)
                return false;
        return true;
    }

    /** The values of {@code row} in a VALUES clause. */
    String values(R row) {
        List<String> values = new ArrayList<>();
        for (Column column : this.columns)
            values.add(literal(column.read(row)));
        return "(" + String.join(", ", values) + ")";
    }

    /** The SQL literal of a column value; NULL for null. */
    static String literal(@Nullable Object value) {
        if (value instanceof LocalDateTime timestamp)
            return "TIMESTAMP '" + timestamp.format(TIMESTAMP_FORMAT) + "'";
        if (value instanceof LocalDate date)
            return "DATE '" + date + "'";
        return String.valueOf(value);
    }

    /** The statement {@code command} applied to {@code row}, once for each table. */
    String statements(String command, R row) {
        StringBuilder result = new StringBuilder();
        for (String prefix : PREFIXES)
            result.append(command).append(prefix).append(this.name)
                    .append(" VALUES ").append(this.values(row)).append(";\n");
        return result.toString();
    }
}

package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** A pair of tables with the same columns: table LATE_name with LATENESS on some columns and
 * table PLAIN_name without; every change goes to both tables or to neither.
 * @param <R>  Type of a row. */
public final class TablePair<R extends Record> {
    /** The prefixes of the names of the two tables. */
    public static final String[] PREFIXES = { "LATE_", "PLAIN_" };

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
            try {
                return this.component.getAccessor().invoke(row);
            } catch (IllegalAccessException | InvocationTargetException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public final String name;
    /** True if the boxed columns of the tables accept NULL. */
    public final boolean nullable;
    final List<Column> columns = new ArrayList<>();
    /** The columns of the LATE_ table that have LATENESS. */
    public final LatenessSchema<R> schema = new LatenessSchema<>();

    /** @param name         Suffix of the table names: the tables are LATE_name and PLAIN_name.
     *  @param nullable     True if the boxed columns of the tables accept NULL.
     *  @param rowClass     The record whose components are the columns of the tables.
     *  @param lateColumns  Names of the columns with LATENESS in the LATE_ table; each is an INT column.
     *  @param lateness     The LATENESS of these columns. */
    public TablePair(String name, boolean nullable, Class<R> rowClass, Set<String> lateColumns, int lateness) {
        this.name = name;
        this.nullable = nullable;
        for (RecordComponent component : rowClass.getRecordComponents()) {
            // The row records are nested in the test classes, which are in another package
            component.getAccessor().setAccessible(true);
            Column column = new Column(component, sqlType(component.getType()));
            this.columns.add(column);
            if (lateColumns.contains(column.name())) {
                // Currently only INT columns can have lateness
                Utilities.enforce(column.sqlType.equals("INT"),
                        () -> "Column " + column.name() + " with LATENESS is not an INT");
                this.schema.addColumn(new IntegerColumn<R>(column.name(), row -> (Integer) column.read(row), lateness));
            }
        }
        for (String late : lateColumns)
            Utilities.enforce(this.schema.hasLateness(late), () -> "No column " + late + " in " + rowClass);
    }

    /** The SQL type of a column for a record component of type {@code type}. */
    static String sqlType(Class<?> type) {
        if (type == int.class || type == Integer.class)
            return "INT";
        if (type == long.class || type == Long.class)
            return "BIGINT";
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
            values.add(String.valueOf(column.read(row)));
        return "(" + String.join(", ", values) + ")";
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

package org.dbsp.sqlCompiler.compiler.sql.suites.nexmark;

import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.simple.IChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPStringLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeString;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Utilities;

import java.nio.charset.StandardCharsets;

public class OneSetChange implements IChange {
    final DBSPZSetLiteral data;

    public OneSetChange(DBSPType elementType) {
        this.data = DBSPZSetLiteral.emptyWithElementType(elementType);
    }

    public OneSetChange add(DBSPExpression expression) {
        this.data.add(expression);
        return this;
    }

    DBSPExpression parseValue(DBSPType fieldType, String data) {
        String trimmed = data.trim();
        DBSPExpression result;
        if (!fieldType.is(DBSPTypeString.class) &&
                (trimmed.isEmpty() ||
                        trimmed.equalsIgnoreCase("null"))) {
            result = fieldType.nullValue();
        } else if (fieldType.is(DBSPTypeTimestamp.class)) {
            long value = Long.parseLong(trimmed);
            result = new DBSPTimestampLiteral(value, fieldType.mayBeNull);
        } else if (fieldType.is(DBSPTypeInteger.class)) {
            return new DBSPI64Literal(Long.parseLong(trimmed), fieldType.mayBeNull);
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
        } else {
            throw new UnimplementedException(fieldType);
        }
        return result;
    }

    DBSPTupleExpression parse(String line) {
        DBSPTypeTuple rowType = this.getSetElementType(0).to(DBSPTypeTuple.class);
        String[] columns;
        if (rowType.size() > 1) {
            columns = line.split(",");
        } else {
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

    public OneSetChange add(String data) {
        return this.add(this.parse(data));
    }

    @Override
    public IChange simplify() {
        return this;
    }

    @Override
    public int setCount() {
        return 1;
    }

    @Override
    public DBSPZSetLiteral getSet(int index) {
        if (index != 0)
            throw new RuntimeException("Only 1 set exists");
        return this.data;
    }
}

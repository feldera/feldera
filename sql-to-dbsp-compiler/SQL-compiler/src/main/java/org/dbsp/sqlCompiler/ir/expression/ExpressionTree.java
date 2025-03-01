package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Use reflection to print an expression as a tree */
public class ExpressionTree {
    static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        while (clazz != null) {
            Collections.addAll(fields, clazz.getDeclaredFields());
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    private static void asTree(DBSPExpression expression, IIndentStream stream) throws IllegalAccessException {
        Class<?> clazz = expression.getClass();
        stream.append(expression.id)
                .append(" ")
                .append(clazz.getSimpleName())
                .increase();
        for (Field field : getAllFields(clazz)) {
            if (DBSPExpression.class.isAssignableFrom(field.getType())) {
                asTree((DBSPExpression) field.get(expression), stream);
            } else if (field.getType().isArray()) {
                Object[] values = (Object[])field.get(expression);
                for (Object obj: values) {
                    if (obj instanceof DBSPExpression)
                        asTree((DBSPExpression) obj, stream);
                }
            }
        }
        stream.decrease();
    }

    public static String asTree(DBSPExpression expression) {
        try {
            IndentStream stream = new IndentStreamBuilder();
            asTree(expression, stream);
            return stream.toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}

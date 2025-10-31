package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.IndentStream;
import org.dbsp.util.IndentStreamBuilder;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Use reflection to print an expression as a tree */
public class ExpressionTree {
    private ExpressionTree() {}

    static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        while (clazz != null) {
            Collections.addAll(fields, clazz.getDeclaredFields());
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    static boolean acceptableType(Class<?> clazz) {
        return int.class.isAssignableFrom(clazz) ||
                String.class.isAssignableFrom(clazz) ||
                boolean.class.isAssignableFrom(clazz) ||
                DBSPOpcode.class.isAssignableFrom(clazz);
    }

    private static void asTree(@Nullable IDBSPInnerNode node, IIndentStream stream) throws IllegalAccessException {
        if (node == null) {
            return;
        }
        Class<?> clazz = node.getClass();
        stream.append(node.getId())
                .append(" ")
                .append(clazz.getSimpleName());
        for (Field field : getAllFields(clazz)) {
            if (!Modifier.isStatic(field.getModifiers()) && acceptableType(field.getType())) {
                field.setAccessible(true);
                stream.append(" ")
                        .append(field.get(node).toString());
            }
        }

        if (node.getNode().getPositionRange().isValid()) {
            stream.append(node.getNode().getPositionRange().toShortString());
        }
        stream.increase();
        for (Field field : getAllFields(clazz)) {
            if (DBSPExpression.class.isAssignableFrom(field.getType())) {
                field.setAccessible(true);
                asTree((DBSPExpression) field.get(node), stream);
            } else if (DBSPStatement.class.isAssignableFrom(field.getType())) {
                field.setAccessible(true);
                asTree((DBSPStatement) field.get(node), stream);
            } else if (field.getType().isArray()) {
                field.setAccessible(true);
                Object[] values = (Object[])field.get(node);
                if (values != null) {
                    for (Object obj : values) {
                        field.setAccessible(true);
                        if (obj instanceof DBSPExpression || obj instanceof DBSPStatement ||
                                obj instanceof DBSPParameter)
                            asTree((IDBSPInnerNode) obj, stream);
                    }
                }
            }  else if (List.class.isAssignableFrom(field.getType())) {
                field.setAccessible(true);
                List<?> values = (List<?>)field.get(node);
                if (values != null) {
                    for (Object obj : values) {
                        field.setAccessible(true);
                        if (obj instanceof DBSPExpression || obj instanceof DBSPStatement)
                            asTree((IDBSPInnerNode) obj, stream);
                    }
                }
            }
        }
        stream.decrease();
    }

    @CheckReturnValue
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

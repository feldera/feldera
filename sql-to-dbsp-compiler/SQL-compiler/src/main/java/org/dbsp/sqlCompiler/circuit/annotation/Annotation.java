package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.util.ICastable;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Base class for annotations that can be attached to various IR nodes. */
public abstract class Annotation implements ICastable {
    /** True for annotations that we don't want displayed in circuit dumps */
    public boolean invisible() { return false; }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this).endObject();
    }

    /** Dispatches to the static fromJson method that every annotation class defines. */
    @SuppressWarnings("unused")
    public static Annotation fromJson(JsonNode node) throws ClassNotFoundException {
        Utilities.enforce(node.isObject());
        ObjectNode object = (ObjectNode) node;
        JsonNode cls = object.get("class");
        Utilities.enforce(cls != null,
                () -> "Node does not have 'class' field: " + Utilities.toDepth(node, 1));
        Class<?> clazz = Class.forName("org.dbsp.sqlCompiler.circuit.annotation." + cls.asText());
        try {
            Method method = clazz.getMethod("fromJson", JsonNode.class);
            Utilities.enforce(method.getDeclaringClass() != Annotation.class,
                    () -> "Annotation " + cls.asText() + " does not define fromJson");
            return (Annotation) method.invoke(null, node);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}

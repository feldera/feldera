package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Utilities;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Deserialize data serialized by either {@link ToJsonOuterVisitor}
 * or {@link ToJsonInnerVisitor}. */
public class JsonDecoder {
    static class Cache<T extends IDBSPNode> {
        Map<Long, IDBSPNode> decoded;

        Cache() {
            this.decoded = new HashMap<>();
        }

        public <S extends T> S lookup(Long id, Class<S> tClass) {
            if (this.decoded.containsKey(id))
                return this.decoded.get(id).to(tClass);
            throw new RuntimeException("Could not find node with id " + id);
        }

        public void cache(Long originalId, IDBSPNode result) {
            Utilities.putNew(this.decoded, originalId, result);
        }
    }

    Cache<IDBSPInnerNode> inner;
    Cache<IDBSPOuterNode> outer;
    public final RelDataTypeFactory typeFactory;

    public JsonDecoder(RelDataTypeFactory typeFactory) {
        this.inner = new Cache<>();
        this.outer = new Cache<>();
        this.typeFactory = typeFactory;
    }

    static final String OUTER_ROOT = "org.dbsp.sqlCompiler.circuit";
    static final List<String> OUTER_PACKAGES = Arrays.asList(
            "", "operator");
    static final String INNER_ROOT = "org.dbsp.sqlCompiler.ir";
    static final List<String> INNER_PACKAGES = Arrays.asList(
            "", "expression", "type.primitive", "type.derived", "type.user",
            "path", "expression.literal", "pattern", "statement");

    static Class<?> getClass(String simpleName, boolean outer) {
        if (simpleName.equals("Field"))
            // Special cases for inner classes
            return DBSPTypeStruct.Field.class;
        String root;
        List<String> toTry;
        if (outer) {
            root = OUTER_ROOT;
            toTry = OUTER_PACKAGES;
        } else {
            root = INNER_ROOT;
            toTry = INNER_PACKAGES;
        }
        for (String cls : toTry) {
            String className = root;
            if (!cls.isEmpty())
                className += "." + cls;
            className += "." + simpleName;
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException ignored) {
            }
        }
        throw new RuntimeException("Class " + Utilities.singleQuote(simpleName) + " not found");
    }

    IDBSPNode decode(JsonNode node, boolean outer) {
        Utilities.enforce(node.isObject());
        ObjectNode object = (ObjectNode) node;
        JsonNode nodeProp = object.get("node");
        if (nodeProp != null) {
            Long id = nodeProp.asLong();
            if (outer)
                return this.outer.lookup(id, IDBSPOuterNode.class);
            else
                return this.inner.lookup(id, IDBSPInnerNode.class);
        }
        Long originalId = Utilities.getLongProperty(node, "id");
        JsonNode cls = object.get("class");
        Utilities.enforce(cls != null, "Node does not have 'class' field: " + Utilities.toDepth(node, 1));
        Class<?> clazz = getClass(cls.asText(), outer);
        try {
            Method method = clazz.getMethod("fromJson", JsonNode.class, JsonDecoder.class);
            // Check if the method is static
            boolean isStatic = Modifier.isStatic(method.getModifiers());
            if (!isStatic)
                throw new RuntimeException(cls + ".fromJson is not static");
            IDBSPNode result = (IDBSPNode) method.invoke(null, node, this);
            if (outer)
                this.outer.cache(originalId, result);
            else
                this.inner.cache(originalId, result);
            return result;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends IDBSPOuterNode> T decodeOuter(JsonNode node, Class<T> clazz) {
        return this.decode(node, true).to(clazz);
    }

    public <T extends IDBSPInnerNode> T decodeInner(JsonNode node, Class<T> clazz) {
        IDBSPNode result = this.decode(node, false);
        return result.to(clazz);
    }
}
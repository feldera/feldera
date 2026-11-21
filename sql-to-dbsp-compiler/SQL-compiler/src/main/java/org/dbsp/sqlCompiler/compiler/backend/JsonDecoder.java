package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.ProvenanceCodec;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.util.Utilities;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.ir.type.user.StreamKind;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.ProgramMetadata;
import javax.annotation.Nullable;
import java.util.ArrayList;

/** Deserialize data serialized by either {@link ToJsonOuterVisitor}
 * or {@link ToJsonInnerVisitor}. */
public class JsonDecoder {
    static class Cache<T extends IDBSPNode> {
        final Map<Long, IDBSPNode> decoded;

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

        /** True if {@code json} is a reference, or a node already decoded. */
        public boolean isDecoded(JsonNode json) {
            if (json.has("node"))
                return true;
            return this.decoded.containsKey(Utilities.getLongProperty(json, "id"));
        }
    }

    private final ProvenanceCodec.Decoder provenance =
            new ProvenanceCodec.Decoder();
    final Cache<IDBSPInnerNode> inner;
    final Cache<IDBSPOuterNode> outer;
    @Nullable private final ProgramMetadata programMetadata;
    /** True while decoding an incremental circuit, whose sources carry deltas */
    boolean incremental = false;
    /** True while decoding the operators inside a nested operator, which all carry deltas */
    boolean nested = false;

    public JsonDecoder() {
        this(null);
    }

    /** Use the resumed compiler's metadata so its passes and circuit share schema mutations. */
    public JsonDecoder(@Nullable ProgramMetadata programMetadata) {
        this.inner = new Cache<>();
        this.outer = new Cache<>();
        this.programMetadata = programMetadata;
    }

    /** In-memory optimizer snapshots can retain identity shared by several flatmaps. */
    public org.dbsp.util.Shuffle decodeShuffle(JsonNode flatmap) {
        return org.dbsp.util.Shuffle.fromJson(Utilities.getProperty(flatmap, "shuffle"));
    }

    public ProgramMetadata decodeProgramMetadata(JsonNode node) {
        if (this.programMetadata == null)
            return ProgramMetadata.fromJson(node);
        this.programMetadata.readSchema(node);
        return this.programMetadata;
    }

    /** Reconnect compiler-owned state to a decoded node. If the circuit does
     * not use the weight variable yet, retain its name for later expansion. */
    public DBSPVariablePath decodeWeightVariable(JsonNode binding) {
        String name = Utilities.getStringProperty(binding, "variable");
        if (!binding.has("node"))
            return new DBSPVariablePath(name, DBSPTypeWeight.INSTANCE);
        DBSPVariablePath variable = this.inner.lookup(
                Utilities.getLongProperty(binding, "node"), DBSPVariablePath.class);
        Utilities.enforce(variable.variable.equals(name) && variable.getType() == DBSPTypeWeight.INSTANCE,
                () -> "Compiler weight variable does not match its circuit binding");
        return variable;
    }

    /** Restore the table identifier separately from the table's relational provenance.
     * An unused table may have no relational references but still has a source location. */
    public static CalciteObject decodeSourceName(JsonNode node) {
        if (!node.has("sourceNamePosition"))
            return CalciteObject.EMPTY;
        SourcePositionRange position = SourcePositionRange.fromJson(node.get("sourceNamePosition"));
        return Utilities.getBooleanProperty(node, "sourceNameEmpty")
                ? new CalciteObject(position) : CalciteObject.create(position);
    }

    /** Only internal seam files mark which definitions refer to built-in singletons.
     * Unmarked definitions must remain distinct even when their values are equal. */
    public static boolean useSingleton(JsonNode node) {
        boolean singleton = node.has("singleton") && Utilities.getBooleanProperty(node, "singleton");
        if (singleton && node.has("position"))
            throw new IllegalArgumentException("A built-in singleton cannot carry a source position");
        return singleton;
    }

    static final String OUTER_ROOT = "org.dbsp.sqlCompiler.circuit";
    static final List<String> OUTER_PACKAGES = Arrays.asList(
            "", "operator");
    static final String INNER_ROOT = "org.dbsp.sqlCompiler.ir";
    static final List<String> INNER_PACKAGES = Arrays.asList(
            "", "expression", "type.primitive", "type.derived", "type.user",
            "path", "expression.literal", "pattern", "statement", "aggregate");

    // Inner and outer nodes have separate class namespaces. Only successful lookups
    // are cached, so the cache is bounded by the compiler's node classes.
    private static final Map<String, Method> INNER_READERS = new ConcurrentHashMap<>();
    private static final Map<String, Method> OUTER_READERS = new ConcurrentHashMap<>();

    private static Method reader(String name, boolean outer) {
        return (outer ? OUTER_READERS : INNER_READERS).computeIfAbsent(name, key -> {
            try {
                Method method = getClass(key, outer).getMethod("fromJson", JsonNode.class, JsonDecoder.class);
                if (!Modifier.isStatic(method.getModifiers()))
                    throw new IllegalArgumentException(key + ".fromJson is not static");
                return method;
            } catch (NoSuchMethodException error) {
                throw new IllegalArgumentException(key + " has no static fromJson(JsonNode, JsonDecoder) method", error);
            }
        });
    }

    private static Class<?> getClass(String simpleName, boolean outer) {
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
        Utilities.enforce(cls != null,
                () -> "Node does not have 'class' field: " + Utilities.toDepth(node, 1));
        Method method = reader(cls.asText(), outer);
        try {
            // The writer emits parent provenance before nested operators. Decode
            // it first too: a child can refer to the parent's provenance node.
            CalciteRelNode savedProvenance = outer && object.has("provenance")
                    ? this.provenance.decode(object.get("provenance")) : null;
            IDBSPNode result = (IDBSPNode) method.invoke(null, node, this);
            // The JSON carries at most one position per node: the inner IR JSON writer emits the single
            // position of the node's CalciteObject.
            // The internal seam restores operator position lists separately below;
            // public circuit JSON does not include those lists.
            // A node that already has a position was built by an enclosing fromJson from one of
            // its children and keeps it.  The fromJson methods that return process-wide singletons
            // (DBSPTypeAny.INSTANCE, DBSPVariantNullLiteral.INSTANCE) are safe: those classes have
            // no constructor taking a Calcite object, so their JSON never carries a position.
            JsonNode position = object.get("position");
            if (!outer && object.has("sourceEmpty") && result instanceof DBSPNode dbspNode) {
                boolean empty = Utilities.getBooleanProperty(object, "sourceEmpty");
                SourcePositionRange range = position == null ? SourcePositionRange.INVALID : SourcePositionRange.fromJson(position);
                if (ReplayTypes.isSharedNode(result) && (!empty || range.isValid()))
                    throw new IllegalArgumentException("A shared built-in node cannot carry a source position");
                if (dbspNode.getNode().isEmpty() != empty || !dbspNode.getNode().getPositionRange().equals(range))
                    dbspNode.restoreSourcePosition(range, empty);
            } else if (position != null && result instanceof DBSPNode dbspNode && dbspNode.getNode().isEmpty()) {
                dbspNode.setSourcePosition(SourcePositionRange.fromJson(position));
            }
            if (result instanceof DBSPOperator operator && savedProvenance != null) {
                operator.restoreProvenance(savedProvenance);
            } else if (result instanceof DBSPOperator operator && object.has("sourcePositions")) {
                List<SourcePositionRange> positions = new ArrayList<>();
                for (JsonNode range: Utilities.getProperty(object, "sourcePositions"))
                    positions.add(SourcePositionRange.fromJson(range));
                operator.restoreSourcePositions(
                        SourcePositionRange.fromJson(Utilities.getProperty(object, "sourcePosition")), positions,
                        Utilities.getBooleanProperty(object, "sourceEmpty"));
            }
            if (outer) {
                this.outer.cache(originalId, result);
            } else {
                this.inner.cache(originalId, result);
                // The fromJson of an expression whose type follows from its children does not read
                // the "type" property; decode it, so that a later reference to the type resolves.
                JsonNode type = object.get("type");
                if (result instanceof DBSPExpression && type != null && !this.inner.isDecoded(type))
                    this.decodeInner(type, DBSPType.class);
            }
            return result;
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error decoding " + cls.asText() + " with id " + originalId, e.getCause());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Class " + cls.asText() + " has no static fromJson(JsonNode, JsonDecoder) method", e);
        }
    }

    /** Called by DBSPCircuit.fromJson before it decodes the operators. */
    public void setIncremental(boolean incremental) {
        this.incremental = incremental;
    }

    /** Called by DBSPNestedOperator.fromJson around the decoding of its operators;
     * returns the previous value, to restore afterwards. */
    public boolean setNested(boolean nested) {
        boolean previous = this.nested;
        this.nested = nested;
        return previous;
    }

    /** The kind of stream a source operator decoded at this point produces.  The kind is not
     * serialized: it follows from the circuit mode and from the position of the operator, so
     * the JSON hashed into persistent ids is unchanged. */
    public StreamKind sourceKind() {
        return (this.incremental || this.nested) ? StreamKind.DELTA : StreamKind.COLLECTION;
    }

    public <T extends IDBSPOuterNode> T decodeOuter(JsonNode node, Class<T> clazz) {
        return this.decode(node, true).to(clazz);
    }

    public <T extends IDBSPInnerNode> T decodeInner(JsonNode node, Class<T> clazz) {
        IDBSPNode result = this.decode(node, false);
        return result.to(clazz);
    }
}

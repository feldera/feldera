package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPFold;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPMinMax;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPComparatorItem;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeWeight;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.JsonStream;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Harvests the expression IR of compiled test circuits into per-expression JSON
 * records, the raw material for the standalone expression oracle.
 *
 * <p>Gated on the {@code FELDERA_EXPR_ORACLE_DIR} environment variable, so it runs
 * only when explicitly collecting a corpus and never perturbs a normal test run.
 * Every failure is swallowed: harvesting must not break the test that triggered it.
 *
 * <p>Every operator (top-level and nested) is visited, and every public
 * {@code DBSPExpression}-typed field on it is a harvest candidate. Four record kinds:
 * <ul>
 *   <li>{@code closure}: a plain {@code DBSPClosureExpression} of any arity whose
 *       parameters are sampleable (tuples of scalar leaves, by reference or value,
 *       unit tuples, the fold {@code Weight});</li>
 *   <li>{@code fold}: a {@code DBSPFold} aggregate, decomposed into its zero,
 *       increment, and postProcess pieces;</li>
 *   <li>{@code minmax}: a {@code DBSPMinMax} aggregate (the aggregation kind plus the
 *       optional postProcessing closure);</li>
 *   <li>{@code const}: a constant Z-set relation ({@code DBSPZSetExpression}).</li>
 * </ul>
 * Closures may reference interned-string and decimal STATIC constants and generated
 * comparator structs; the referenced declarations travel with the record so the
 * generated crate can emit them. Unsupported shapes are skipped and counted, so the
 * generated crate always compiles.
 */
public final class ExpressionOracleHarvest {
    private ExpressionOracleHarvest() {}

    /** Rust leaf types the oracle runtime implements `Sample` + `ToOracleJson` for. */
    private static final Set<String> SUPPORTED_LEAF = Set.of(
            "bool", "i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64", "u128", "F32",
            "F64", "SqlString", "Date", "Time", "Timestamp", "TimestampTz", "ShortInterval",
            "LongInterval", "ByteArray", "Uuid", "Variant", "GeoPoint");

    /** The most leaf columns a single sampled parameter may contribute. */
    private static final int MAX_PARAM_LEAVES = 10;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Coverage counters, accumulated across the suite (surefire reuses one JVM) and
    // rewritten to `_stats.json` after every circuit so the last write is the total.
    private static final AtomicInteger candidates = new AtomicInteger();
    private static final AtomicInteger emitted = new AtomicInteger();
    private static final Map<String, AtomicInteger> skipped = new ConcurrentHashMap<>();

    private static void skip(String reason) {
        skipped.computeIfAbsent(reason, ignored -> new AtomicInteger()).incrementAndGet();
    }

    public static void maybeHarvest(DBSPCircuit circuit, DBSPCompiler compiler) {
        String dir = System.getenv("FELDERA_EXPR_ORACLE_DIR");
        if (dir == null || dir.isBlank()) {
            return;
        }
        try {
            harvest(circuit, compiler, Paths.get(dir));
        } catch (Throwable ignored) {
            // Harvesting is best-effort; a normal test run must not depend on it.
        }
    }

    private static void harvest(DBSPCircuit circuit, DBSPCompiler compiler, Path dir)
            throws Exception {
        Files.createDirectories(dir);
        Map<DBSPOperator, Set<String>> provenance = sqlProvenance(circuit);

        for (DBSPOperator operator : allOperators(circuit)) {
            for (NamedExpression named : expressionFields(operator)) {
                candidates.incrementAndGet();
                try {
                    harvestExpression(circuit, compiler, dir, operator, named, provenance);
                } catch (Throwable t) {
                    // One bad expression must not lose the rest of the circuit.
                    skip("harvest_error_" + t.getClass().getSimpleName());
                }
            }
        }
        writeStats(dir);
    }

    /** Every operator in the circuit, recursing into nested (recursive-CTE) operators. */
    private static List<DBSPOperator> allOperators(DBSPCircuit circuit) {
        List<DBSPOperator> all = new ArrayList<>();
        Deque<DBSPOperator> work = new ArrayDeque<>();
        circuit.getAllOperators().forEach(work::add);
        while (!work.isEmpty()) {
            DBSPOperator operator = work.removeFirst();
            all.add(operator);
            if (operator instanceof DBSPNestedOperator nested) {
                nested.getAllOperators().forEach(work::add);
            }
        }
        return all;
    }

    private record NamedExpression(String field, DBSPExpression expression) {}

    /**
     * Every public non-static {@code DBSPExpression}-typed field of the operator that
     * holds a value: {@code function}, {@code postProcess}, {@code init},
     * {@code extractTs}, {@code error}, and so on. Reflection keeps this complete as
     * operator classes grow fields; non-harvestable expression kinds are skipped (and
     * counted) downstream.
     */
    private static List<NamedExpression> expressionFields(DBSPOperator operator) {
        List<NamedExpression> result = new ArrayList<>();
        Field[] fields = operator.getClass().getFields();
        Arrays.sort(fields, Comparator.comparing(Field::getName));
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())
                    || !DBSPExpression.class.isAssignableFrom(field.getType())) {
                continue;
            }
            try {
                Object value = field.get(operator);
                if (value != null) {
                    result.add(new NamedExpression(field.getName(), (DBSPExpression) value));
                }
            } catch (IllegalAccessException ignored) {
                // A non-accessible field is not part of the harvestable surface.
            }
        }
        return result;
    }

    /** Dispatch one (operator, field, expression) candidate on the expression kind. */
    private static void harvestExpression(
            DBSPCircuit circuit, DBSPCompiler compiler, Path dir, DBSPOperator operator,
            NamedExpression named, Map<DBSPOperator, Set<String>> provenance) throws Exception {
        ObjectNode record;
        if (named.expression instanceof DBSPClosureExpression closure) {
            record = closureRecord(compiler, closure);
        } else if (named.expression instanceof DBSPFold fold) {
            record = foldRecord(compiler, fold);
        } else if (named.expression instanceof DBSPMinMax minMax) {
            record = minMaxRecord(compiler, operator, minMax);
        } else if (named.expression instanceof DBSPZSetExpression zset) {
            record = constRecord(compiler, zset);
        } else {
            skip("unsupported_expression_" + named.expression.getClass().getSimpleName());
            return;
        }
        if (record == null) {
            return; // the builder counted the specific skip reason
        }

        // The Rust text of every piece is both the dedup key and the declaration probe.
        StringBuilder allRust = new StringBuilder();
        record.fields().forEachRemaining(entry -> {
            if (entry.getKey().startsWith("rust_") && entry.getValue().isTextual()) {
                allRust.append(entry.getValue().asText()).append('\n');
            }
        });
        List<DBSPDeclaration> declarations = referencedDeclarations(circuit, allRust.toString());
        if (declarations == null) {
            // References a declaration that is neither a STATIC constant nor a generated
            // comparator (a user-defined function or struct); emitting those is out of scope.
            skip("non_static_declaration");
            return;
        }

        record.put("operator", operator.getClass().getSimpleName());
        record.put("field", named.field);
        attachDeclarations(compiler, declarations, record);
        attachComparators(compiler, named.expression, allRust.toString(), record);
        record.set("ir", buildIr(compiler, irRoot(named.expression), declarations));

        String hash = sha1(allRust.toString());
        record.put("name", "case_" + hash);
        Path file = dir.resolve(hash + ".json");
        // Provenance: the SQL view(s) this expression feeds, unioned with whatever a prior
        // run recorded under the same dedup file, so a case can be traced to its query.
        record.set("generating_sql",
                mergeGeneratingSql(file, provenance.getOrDefault(operator, Set.of())));

        // Filename is the dedup key: identical expressions from different tests and
        // JVMs converge on one file rather than racing an append.
        MAPPER.writeValue(file.toFile(), record);
        emitted.incrementAndGet();
    }

    /** The node serialized as the record's `ir.function`. */
    private static IDBSPInnerNode irRoot(DBSPExpression expression) {
        return expression;
    }

    // ------------------------------------------------------------------
    // closure records
    // ------------------------------------------------------------------

    /**
     * A plain closure of any arity. Each parameter must be sampleable: a tuple of
     * scalar leaves (by reference or by value, possibly the nullable left-join
     * `&Option<Tup>`), a bare supported leaf, a unit tuple, or the fold `Weight`.
     * The legacy map-index shape (one `&(&key, &value)` parameter) is kept as its
     * own representation.
     */
    @Nullable
    private static ObjectNode closureRecord(DBSPCompiler compiler, DBSPClosureExpression closure) {
        ObjectNode record = MAPPER.createObjectNode();
        record.put("kind", "closure");
        int arity = closure.parameters.length;
        boolean index = arity == 1 && isIndexedParam(closure.parameters[0].getType());
        record.put("index", index);
        ArrayNode params = index
                ? indexParams(compiler, closure)
                : closureParams(compiler, closure);
        if (params == null) {
            skip("unsupported_parameter");
            return null;
        }
        if (!isSupportedResult(compiler, closure.getResultType())) {
            skip("unsupported_result");
            return null;
        }
        record.set("params", params);
        record.put("rust_closure", ToRustInnerVisitor.toRustString(compiler, closure, null, false));
        return record;
    }

    @Nullable
    private static ArrayNode closureParams(DBSPCompiler compiler, DBSPClosureExpression closure) {
        ArrayNode params = MAPPER.createArrayNode();
        for (var parameter : closure.parameters) {
            ObjectNode param = paramSpec(compiler, parameter.getType());
            if (param == null) {
                return null;
            }
            params.add(param);
        }
        return params;
    }

    /**
     * Classify one closure parameter and describe how to build a probe value for it:
     * a `ctor` template over `{i}` leaf placeholders plus the leaf types, or a unit /
     * weight marker. Null if the parameter is not sampleable.
     */
    @Nullable
    private static ObjectNode paramSpec(DBSPCompiler compiler, DBSPType type) {
        boolean ref = false;
        if (type instanceof DBSPTypeRef refType) {
            ref = true;
            type = refType.type;
        }
        ObjectNode param = MAPPER.createObjectNode();
        param.put("mode", ref ? "ref" : "value");
        if (type instanceof DBSPTypeWeight) {
            param.put("kind", "weight");
            return param;
        }
        if (type instanceof DBSPTypeTupleBase tuple && tuple.tupFields.length == 0) {
            param.put("kind", "unit");
            param.put("ctor", tuple instanceof DBSPTypeRawTuple ? "()" : "Tup0::new()");
            return param;
        }
        boolean nullable = type.mayBeNull && type instanceof DBSPTypeTuple;
        Shape shape = shape(compiler, nullable ? type.withMayBeNull(false) : type);
        if (shape == null || shape.leaves.size() > MAX_PARAM_LEAVES) {
            return null;
        }
        // A nullable by-value tuple would need Option construction the adapters do not
        // do; the nullable case is the left join's borrowed `&Option<Tup>` only.
        if (nullable && !ref) {
            return null;
        }
        param.put("kind", "tuple");
        param.put("nullable", nullable);
        param.put("ctor", shape.ctor);
        ArrayNode leaves = param.putArray("leaves");
        shape.leaves.forEach(leaves::add);
        return param;
    }

    private record Shape(String ctor, List<String> leaves) {}

    /**
     * A constructor template for one sampled value: tuples become `TupN::new(..)`,
     * raw tuples become native `(..)` tuples, and each supported leaf becomes a
     * `{i}` placeholder. Null if any leaf is unsupported or a nested tuple is
     * nullable (Option construction is not generated for inner tuples).
     */
    @Nullable
    private static Shape shape(DBSPCompiler compiler, DBSPType type) {
        if (type instanceof DBSPTypeTupleBase tuple) {
            if (type.mayBeNull) {
                return null;
            }
            List<String> leaves = new ArrayList<>();
            List<String> parts = new ArrayList<>();
            for (DBSPType field : tuple.tupFields) {
                Shape child = shape(compiler, field);
                if (child == null) {
                    return null;
                }
                // Re-number the child's placeholders after the leaves already collected.
                String ctor = child.ctor;
                for (int i = child.leaves.size() - 1; i >= 0; i--) {
                    ctor = ctor.replace("{" + i + "}", "{" + (leaves.size() + i) + "}");
                }
                leaves.addAll(child.leaves);
                parts.add(ctor);
            }
            String joined = String.join(", ", parts);
            String ctor = tuple instanceof DBSPTypeRawTuple
                    ? "(" + joined + ")"
                    : "Tup" + tuple.tupFields.length + "::new(" + joined + ")";
            return new Shape(ctor, leaves);
        }
        String rust = ToRustInnerVisitor.toRustString(compiler, type, null, false);
        if (!isSupportedLeaf(rust)) {
            return null;
        }
        return new Shape("{0}", List.of(rust));
    }

    /** Whether the parameter is a borrowed key/value pair `&(&keyTuple, &valueTuple)`. */
    private static boolean isIndexedParam(DBSPType type) {
        return type instanceof DBSPTypeRef ref
                && ref.type instanceof DBSPTypeRawTuple pair
                && pair.tupFields.length == 2
                && pair.tupFields[0] instanceof DBSPTypeRef;
    }

    /**
     * A closure over an indexed input takes one parameter, a borrowed key/value pair
     * {@code &(&keyTuple, &valueTuple)}. The two tuples become the parameter list.
     */
    @Nullable
    private static ArrayNode indexParams(DBSPCompiler compiler, DBSPClosureExpression closure) {
        DBSPTypeRawTuple pair =
                (DBSPTypeRawTuple) ((DBSPTypeRef) closure.parameters[0].getType()).type;
        ArrayNode params = MAPPER.createArrayNode();
        for (DBSPType field : pair.tupFields) {
            if (!(field instanceof DBSPTypeRef inner) || inner.type.mayBeNull) {
                return null;
            }
            ObjectNode param = paramSpec(compiler, inner.type);
            if (param == null || !"tuple".equals(param.get("kind").asText())) {
                return null;
            }
            params.add(param);
        }
        return params;
    }

    // ------------------------------------------------------------------
    // fold records
    // ------------------------------------------------------------------

    /**
     * A GROUP BY aggregate: zero, increment `|acc: &mut A, row: &V, w: Weight|`, and
     * postProcess `|acc: A| -> O`. The pieces are recorded separately; the oracle
     * driver replays the fold over sampled (row, weight) sequences and records the
     * post-processed running output per prefix. The accumulator type only has to
     * compile (a `Vec` accumulator is fine), never to be sampled or encoded.
     */
    @Nullable
    private static ObjectNode foldRecord(DBSPCompiler compiler, DBSPFold fold) {
        DBSPClosureExpression increment = fold.increment;
        if (increment.parameters.length != 3) {
            skip("fold_increment_arity");
            return null;
        }
        if (!(increment.parameters[0].getType() instanceof DBSPTypeRef accRef)
                || !accRef.mutable) {
            skip("fold_acc_shape");
            return null;
        }
        if (!(increment.parameters[2].getType() instanceof DBSPTypeWeight)) {
            // Rewrites can replace the weight parameter with another type; the driver
            // only knows how to feed a real Weight.
            skip("fold_weight_shape");
            return null;
        }
        ObjectNode row = paramSpec(compiler, increment.parameters[1].getType());
        if (row == null || !"tuple".equals(row.get("kind").asText())
                || row.get("nullable").asBoolean()) {
            skip("unsupported_parameter");
            return null;
        }
        DBSPClosureExpression post = fold.postProcess;
        if (post.parameters.length != 1
                || !isSupportedResult(compiler, post.getResultType())) {
            skip("unsupported_result");
            return null;
        }
        ObjectNode record = MAPPER.createObjectNode();
        record.put("kind", "fold");
        ArrayNode params = record.putArray("params");
        params.add(row);
        record.put("acc_type", ToRustInnerVisitor.toRustString(compiler, accRef.type, null, false));
        record.put("post_mode",
                post.parameters[0].getType() instanceof DBSPTypeRef ? "ref" : "value");
        record.put("rust_zero", ToRustInnerVisitor.toRustString(compiler, fold.zero, null, false));
        record.put("rust_increment", ToRustInnerVisitor.toRustString(compiler, increment, null, false));
        record.put("rust_post", ToRustInnerVisitor.toRustString(compiler, post, null, false));
        return record;
    }

    // ------------------------------------------------------------------
    // minmax records
    // ------------------------------------------------------------------

    /**
     * A MIN/MAX aggregate: the DBSP aggregation kind (`Min`, `Max`, `MinSome1`,
     * `ArgMinSome`) plus the optional postProcessing closure. The oracle driver
     * mirrors the aggregator contract over sampled (value, weight) sequences.
     *
     * <p>The sampled row type is the operator input's indexed Z-set value type: with a
     * postProcessing closure the DBSPMinMax function type describes the SQL output, not
     * the value the aggregator scans.
     */
    @Nullable
    private static ObjectNode minMaxRecord(
            DBSPCompiler compiler, DBSPOperator operator, DBSPMinMax minMax) {
        DBSPType valueType;
        try {
            valueType = operator.inputs.get(0).getOutputIndexedZSetType().elementType;
        } catch (RuntimeException shape) {
            skip("minmax_input_shape");
            return null;
        }
        ObjectNode row = paramSpec(compiler, valueType);
        if (row == null || !"tuple".equals(row.get("kind").asText())
                || row.get("nullable").asBoolean()) {
            skip("unsupported_parameter");
            return null;
        }
        if (!(minMax.getType() instanceof DBSPTypeFunction function)
                || !isSupportedResult(compiler, function.resultType)) {
            skip("unsupported_result");
            return null;
        }
        ObjectNode record = MAPPER.createObjectNode();
        record.put("kind", "minmax");
        record.put("aggregation", minMax.aggregation.name());
        ArrayNode params = record.putArray("params");
        params.add(row);
        if (minMax.postProcessing != null) {
            if (minMax.postProcessing.parameters.length != 1) {
                skip("minmax_post_arity");
                return null;
            }
            record.put("post_mode",
                    minMax.postProcessing.parameters[0].getType() instanceof DBSPTypeRef
                            ? "ref" : "value");
            record.put("rust_post",
                    ToRustInnerVisitor.toRustString(compiler, minMax.postProcessing, null, false));
        }
        // The aggregation kind and value type participate in dedup even though they are
        // not Rust text; without them, MinSome1 over INT and over VARCHAR would collide.
        record.put("rust_aggregation", minMax.aggregation.name() + " over "
                + ToRustInnerVisitor.toRustString(compiler, valueType, null, false));
        return record;
    }

    // ------------------------------------------------------------------
    // const records
    // ------------------------------------------------------------------

    /** A constant relation: the Z-set literal, recorded as rows plus weights. */
    @Nullable
    private static ObjectNode constRecord(DBSPCompiler compiler, DBSPZSetExpression zset) {
        Shape element = shape(compiler, zset.elementType);
        if (element == null) {
            skip("unsupported_const_element");
            return null;
        }
        ObjectNode record = MAPPER.createObjectNode();
        record.put("kind", "const");
        record.put("rust_value", ToRustInnerVisitor.toRustString(compiler, zset, null, false));
        // The element type participates in dedup: every empty constant renders as
        // `zset!()` regardless of its schema.
        record.put("rust_elem_type",
                ToRustInnerVisitor.toRustString(compiler, zset.elementType, null, false));
        return record;
    }

    // ------------------------------------------------------------------
    // shared plumbing
    // ------------------------------------------------------------------

    /**
     * Map each operator to the SQL of the view(s) it feeds, by walking upstream from every
     * sink. One closure can serve several views, so the value is a set of view queries.
     */
    private static Map<DBSPOperator, Set<String>> sqlProvenance(DBSPCircuit circuit) {
        Map<DBSPOperator, Set<String>> provenance = new HashMap<>();
        for (DBSPSinkOperator sink : circuit.sinkOperators.values()) {
            String sql = sink.query == null || sink.query.isBlank()
                    ? sink.viewName.toString()
                    : sink.query;
            Deque<DBSPOperator> work = new ArrayDeque<>();
            Set<DBSPOperator> seen = new HashSet<>();
            work.add(sink);
            seen.add(sink);
            while (!work.isEmpty()) {
                DBSPOperator operator = work.removeFirst();
                provenance.computeIfAbsent(operator, key -> new LinkedHashSet<>()).add(sql);
                for (OutputPort input : operator.inputs) {
                    if (seen.add(input.operator)) {
                        work.add(input.operator);
                    }
                }
            }
        }
        return provenance;
    }

    /**
     * Union this expression's view SQL with whatever a prior run already recorded under the
     * same dedup file, so re-harvesting accumulates provenance rather than overwriting it.
     */
    private static ArrayNode mergeGeneratingSql(Path file, Set<String> fresh) {
        LinkedHashSet<String> all = new LinkedHashSet<>();
        if (Files.exists(file)) {
            try {
                JsonNode prior = MAPPER.readTree(file.toFile()).get("generating_sql");
                if (prior != null && prior.isArray()) {
                    prior.forEach(node -> all.add(node.asText()));
                }
            } catch (Exception ignored) {
                // A malformed prior file is replaced, not merged.
            }
        }
        all.addAll(fresh);
        ArrayNode array = MAPPER.createArrayNode();
        all.forEach(array::add);
        return array;
    }

    /**
     * Coverage so far, rewritten after every circuit. `candidates` counts the operator
     * expression fields visited; `emitted` counts those that passed every filter (before
     * dedup); the unique total is the record-file count.
     */
    private static void writeStats(Path dir) throws Exception {
        ObjectNode stats = MAPPER.createObjectNode();
        stats.put("candidates", candidates.get());
        stats.put("emitted", emitted.get());
        ObjectNode bySkip = stats.putObject("skipped");
        skipped.forEach((reason, count) -> bySkip.put(reason, count.get()));
        MAPPER.writeValue(dir.resolve("_stats.json").toFile(), stats);
    }

    private static boolean isSupportedLeaf(String rustType) {
        String base = stripOption(rustType);
        // An ARRAY column is `Array<element>`; allow it when the element is itself a supported
        // leaf (so the runtime has a Sample/encoding for it). This admits arrays of scalars and
        // nested arrays, but not arrays of rows (a `Tup<...>` element has no sampler).
        if (base.startsWith("Array<") && base.endsWith(">")) {
            String element = base.substring("Array<".length(), base.length() - 1).trim();
            return isSupportedLeaf(element);
        }
        // A waterline bound `TypedBox<T, DynData>` samples and encodes as its inner T.
        if (base.startsWith("TypedBox<") && base.endsWith(">")) {
            String args = base.substring("TypedBox<".length(), base.length() - 1);
            int depth = 0;
            for (int i = 0; i < args.length(); i++) {
                char c = args.charAt(i);
                if (c == '<') depth++;
                else if (c == '>') depth--;
                else if (c == ',' && depth == 0) {
                    return isSupportedLeaf(args.substring(0, i).trim());
                }
            }
            return false;
        }
        return SUPPORTED_LEAF.contains(base) || base.startsWith("SqlDecimal");
    }

    /**
     * The result type the generated crate will Arrow-encode: a scalar (predicate), or a
     * tuple / raw tuple of supported values (possibly nested, a weighable accumulator).
     */
    private static boolean isSupportedResult(DBSPCompiler compiler, DBSPType type) {
        if (type instanceof DBSPTypeRef ref) {
            return isSupportedResult(compiler, ref.type);
        }
        if (type instanceof DBSPTypeTupleBase tuple) {
            for (DBSPType field : tuple.tupFields) {
                if (!isSupportedResult(compiler, field)) {
                    return false;
                }
            }
            return true;
        }
        return isSupportedLeaf(ToRustInnerVisitor.toRustString(compiler, type, null, false));
    }

    private static String stripOption(String rustType) {
        if (rustType.startsWith("Option<") && rustType.endsWith(">")) {
            return rustType.substring("Option<".length(), rustType.length() - 1).trim();
        }
        return rustType;
    }

    /**
     * The circuit declarations the harvested Rust references (by name). STATIC constants
     * and generated comparator structs are allowed; null if it references anything else
     * (a user-defined function or struct), which the generator cannot emit.
     */
    @Nullable
    private static List<DBSPDeclaration> referencedDeclarations(
            DBSPCircuit circuit, String rust) {
        List<DBSPDeclaration> referenced = new ArrayList<>();
        for (DBSPDeclaration declaration : circuit.declarations) {
            String name = declaration.getName();
            if (name.isBlank() || !rust.contains(name)) {
                continue;
            }
            if (!(declaration.item instanceof DBSPStaticItem)
                    && !(declaration.item instanceof DBSPComparatorItem)) {
                return null;
            }
            referenced.add(declaration);
        }
        return referenced;
    }

    /**
     * Render each referenced declaration for the generated crate: a STATIC becomes a
     * function-local declaration plus its initializer; a comparator item becomes a
     * module-level `struct CmpX; impl CmpFunc<..> for CmpX { .. }` item.
     */
    private static void attachDeclarations(
            DBSPCompiler compiler, List<DBSPDeclaration> declarations, ObjectNode record) {
        ArrayNode decls = record.putArray("static_decls");
        ArrayNode inits = record.putArray("static_inits");
        ArrayNode items = record.putArray("item_decls");
        for (DBSPDeclaration declaration : declarations) {
            if (declaration.item instanceof DBSPStaticItem staticItem) {
                var stat = staticItem.expression;
                String name = stat.getName();
                String type = ToRustInnerVisitor.toRustString(compiler, stat.getType(), null, false);
                String init = ToRustInnerVisitor.toRustString(compiler, stat.initializer, null, false);
                decls.add("static " + name + ": StaticLazy<" + type + "> = StaticLazy::new();");
                inits.add(name + ".init(move || " + init + ");");
            } else {
                items.add(ToRustInnerVisitor.toRustString(compiler, declaration.item, null, false));
            }
        }
    }

    /**
     * Emit a `struct CmpX; impl CmpFunc<..> for CmpX { .. }` item for every comparator
     * the harvested Rust references by name (`ARRAY_AGG .. ORDER BY` sorts through a
     * generated struct). At harvest time the comparators are still inline expressions;
     * the Rust backend would materialize the items later, so the record does it here.
     */
    private static void attachComparators(
            DBSPCompiler compiler, DBSPExpression root, String rust, ObjectNode record) {
        List<org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression> comparators =
                new ArrayList<>();
        var collector = new org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor(compiler) {
            @Override
            public void postorder(
                    org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression comparator) {
                comparators.add(comparator);
            }
        };
        root.accept(collector);
        ArrayNode items = (ArrayNode) record.get("item_decls");
        Set<String> emittedStructs = new HashSet<>();
        for (var comparator : comparators) {
            String structName = comparator.getComparatorStructName();
            if (!rust.contains(structName) || !emittedStructs.add(structName)) {
                continue;
            }
            items.add(ToRustInnerVisitor.toRustString(
                    compiler, new DBSPComparatorItem(comparator), null, false));
        }
    }

    /** The expression IR plus the declarations it references, self-contained. */
    private static ObjectNode buildIr(
            DBSPCompiler compiler, IDBSPInnerNode root, List<DBSPDeclaration> declarations)
            throws Exception {
        ArrayNode irDeclarations = MAPPER.createArrayNode();
        for (DBSPDeclaration declaration : declarations) {
            ObjectNode wrapper = MAPPER.createObjectNode();
            wrapper.set("item", MAPPER.readTree(innerJson(compiler, declaration.item)));
            irDeclarations.add(wrapper);
        }
        ObjectNode ir = MAPPER.createObjectNode();
        ir.set("declarations", irDeclarations);
        ir.set("function", MAPPER.readTree(innerJson(compiler, root)));
        return ir;
    }

    private static String innerJson(DBSPCompiler compiler, IDBSPInnerNode node) {
        JsonStream stream = new JsonStream(new IndentStreamBuilder());
        // The corpus is graded by the Gen-2 evaluator, so it takes the Gen-2 JSON form even
        // though the test suite compiles (and runs) the Rust backend without --gen2.
        ToJsonInnerVisitor visitor = new ToJsonInnerVisitor(compiler, stream, 1, true);
        node.accept(visitor);
        return visitor.getJsonString();
    }

    private static String sha1(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-1").digest(value.getBytes("UTF-8"));
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            hex.append(Character.forDigit((b >> 4) & 0xf, 16));
            hex.append(Character.forDigit(b & 0xf, 16));
        }
        return hex.toString();
    }
}

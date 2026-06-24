package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLeftJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPStaticItem;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRef;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.IndentStreamBuilder;
import org.dbsp.util.JsonStream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Harvests stateless scalar closures from compiled test circuits into per-closure
 * JSON records, the raw material for the standalone expression oracle.
 *
 * <p>Gated on the {@code FELDERA_EXPR_ORACLE_DIR} environment variable, so it runs
 * only when explicitly collecting a corpus and never perturbs a normal test run.
 * Every failure is swallowed: harvesting must not break the test that triggered it.
 *
 * <p>Harvested shapes:
 * <ul>
 *   <li>{@code Map}/{@code Filter} (one parameter, {@code &Tup<leaves>}): a projection
 *       or predicate over one row;</li>
 *   <li>equi-join projections ({@code Join}/{@code StreamJoin}/{@code LeftJoin}, three
 *       parameters {@code &key, &left, &right}, each a {@code &Tup<leaves>}).</li>
 * </ul>
 * Every leaf column must be a scalar the oracle runtime can sample, and each tuple at
 * most ten wide. Closures may reference interned-string and decimal STATIC constants;
 * the referenced declarations travel with the record so the generated crate can emit
 * them. Indexed (raw-tuple) parameters and unsupported result shapes are skipped, so
 * the generated crate always compiles.
 */
public final class ExpressionOracleHarvest {
    private ExpressionOracleHarvest() {}

    /** Rust leaf types the oracle runtime implements `Sample` + `ToOracleJson` for. */
    private static final Set<String> SUPPORTED_LEAF = Set.of(
            "bool", "i8", "i16", "i32", "i64", "i128", "u8", "u16", "u32", "u64", "u128", "F32",
            "F64", "SqlString", "Date", "Time", "Timestamp", "TimestampTz", "ShortInterval",
            "LongInterval", "ByteArray", "Uuid", "Variant", "GeoPoint");

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

        for (DBSPOperator operator : circuit.getAllOperators()) {
            if (!(operator instanceof DBSPSimpleOperator simple) || !isHarvestable(operator)) {
                continue;
            }
            DBSPExpression function = simple.function;
            if (!(function instanceof DBSPClosureExpression closure)) {
                continue;
            }
            int arity = closure.parameters.length;
            if (arity != 1 && arity != 3) {
                continue;
            }
            candidates.incrementAndGet();
            // A closure over an indexed input takes a borrowed key/value pair
            // `&(&key, &value)`; everything else takes plain `&Tup` rows.
            boolean index = arity == 1 && isIndexedParam(closure.parameters[0].getType());
            ArrayNode params = index ? indexParams(compiler, closure) : scalarParams(compiler, closure);
            if (params == null) {
                skip("unsupported_parameter");
                continue;
            }
            if (!isSupportedResult(compiler, closure.getResultType())) {
                skip("unsupported_result");
                continue;
            }

            String rustClosure = ToRustInnerVisitor.toRustString(compiler, closure, null, false);
            List<DBSPDeclaration> declarations = referencedDeclarations(circuit, rustClosure);
            if (declarations == null) {
                // References a declaration that is not a STATIC constant (a function or
                // struct); emitting those is out of scope.
                skip("non_static_declaration");
                continue;
            }

            String hash = sha1(rustClosure);
            ObjectNode record = MAPPER.createObjectNode();
            record.put("name", "case_" + hash);
            record.put("operator", operator.getClass().getSimpleName());
            record.put("index", index);
            record.set("params", params);
            attachStatics(compiler, declarations, record);
            record.put("rust_closure", rustClosure);
            record.set("ir", buildIr(compiler, closure, declarations));

            // Filename is the dedup key: identical closures from different tests and
            // JVMs converge on one file rather than racing an append.
            MAPPER.writeValue(dir.resolve(hash + ".json").toFile(), record);
            emitted.incrementAndGet();
        }
        writeStats(dir);
    }

    /**
     * Coverage so far, rewritten after every circuit. `candidates` counts the
     * stateless scalar closures of a harvestable shape; `emitted` counts those that
     * passed every filter (before dedup); the unique total is the record-file count.
     */
    private static void writeStats(Path dir) throws Exception {
        ObjectNode stats = MAPPER.createObjectNode();
        stats.put("candidates", candidates.get());
        stats.put("emitted", emitted.get());
        ObjectNode bySkip = stats.putObject("skipped");
        skipped.forEach((reason, count) -> bySkip.put(reason, count.get()));
        MAPPER.writeValue(dir.resolve("_stats.json").toFile(), stats);
    }

    /** The operator kinds whose closure is a stateless scalar projection or predicate. */
    private static boolean isHarvestable(DBSPOperator operator) {
        return operator instanceof DBSPMapOperator
                || operator instanceof DBSPMapIndexOperator
                || operator instanceof DBSPFilterOperator
                || operator instanceof DBSPJoinOperator
                || operator instanceof DBSPStreamJoinOperator
                || operator instanceof DBSPLeftJoinOperator;
    }

    /**
     * One entry per closure parameter, each `&Tup<scalar leaves>`; null if any parameter
     * is not that shape or a leaf is not sampleable.
     */
    private static ArrayNode scalarParams(DBSPCompiler compiler, DBSPClosureExpression closure) {
        ArrayNode params = MAPPER.createArrayNode();
        for (var parameter : closure.parameters) {
            if (!(parameter.getType() instanceof DBSPTypeRef ref)
                    || !(ref.type instanceof DBSPTypeTuple tuple)) {
                return null;
            }
            ObjectNode param = tupleParam(compiler, tuple, true);
            if (param == null) {
                return null;
            }
            params.add(param);
        }
        return params;
    }

    /** Whether the parameter is a borrowed key/value pair `&(&keyTuple, &valueTuple)`. */
    private static boolean isIndexedParam(DBSPType type) {
        return type instanceof DBSPTypeRef ref
                && ref.type instanceof DBSPTypeRawTuple pair
                && pair.tupFields.length == 2;
    }

    /**
     * A closure over an indexed input takes one parameter, a borrowed key/value pair
     * {@code &(&keyTuple, &valueTuple)}. The two tuples become the parameter list.
     */
    private static ArrayNode indexParams(DBSPCompiler compiler, DBSPClosureExpression closure) {
        if (!(closure.parameters[0].getType() instanceof DBSPTypeRef ref)
                || !(ref.type instanceof DBSPTypeRawTuple pair)
                || pair.tupFields.length != 2) {
            return null;
        }
        ArrayNode params = MAPPER.createArrayNode();
        for (DBSPType field : pair.tupFields) {
            if (!(field instanceof DBSPTypeRef inner)
                    || !(inner.type instanceof DBSPTypeTuple tuple)) {
                return null;
            }
            // A nullable key or value tuple would need Option construction the index
            // driver does not do; skip it.
            ObjectNode param = tupleParam(compiler, tuple, false);
            if (param == null) {
                return null;
            }
            params.add(param);
        }
        return params;
    }

    /** A parameter descriptor for one tuple of scalar leaves, or null if unsupported. */
    private static ObjectNode tupleParam(DBSPCompiler compiler, DBSPTypeTuple tuple, boolean allowNull) {
        if (tuple.mayBeNull && !allowNull) {
            return null;
        }
        DBSPType[] fields = tuple.tupFields;
        if (fields.length < 1 || fields.length > 10) {
            return null;
        }
        ObjectNode param = MAPPER.createObjectNode();
        // A nullable parameter tuple is a left join's absent right row (`&Option<Tup>`);
        // the generator samples it as Some/None.
        param.put("nullable", tuple.mayBeNull);
        ArrayNode leaves = param.putArray("leaves");
        for (DBSPType field : fields) {
            String rust = ToRustInnerVisitor.toRustString(compiler, field, null, false);
            if (!isSupportedLeaf(rust)) {
                return null;
            }
            leaves.add(rust);
        }
        return param;
    }

    private static boolean isSupportedLeaf(String rustType) {
        String base = stripOption(rustType);
        return SUPPORTED_LEAF.contains(base) || base.startsWith("SqlDecimal");
    }

    /**
     * The result type the generated crate will JSON-encode: a scalar (predicate), or a
     * tuple / raw tuple of supported values (a projection or an indexed key/value pair).
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
     * The circuit declarations the closure references (by name). Null if it references a
     * declaration that is not a STATIC constant, which the generator cannot emit.
     */
    private static List<DBSPDeclaration> referencedDeclarations(
            DBSPCircuit circuit, String rustClosure) {
        List<DBSPDeclaration> referenced = new ArrayList<>();
        for (DBSPDeclaration declaration : circuit.declarations) {
            String name = declaration.getName();
            if (name.isBlank() || !rustClosure.contains(name)) {
                continue;
            }
            if (!(declaration.item instanceof DBSPStaticItem)) {
                return null;
            }
            referenced.add(declaration);
        }
        return referenced;
    }

    /**
     * Render each referenced STATIC as a function-local declaration plus its initializer.
     * The generated crate emits all declarations first, then all initializers (init is
     * idempotent and the value is computed lazily), then the closure that reads them.
     */
    private static void attachStatics(
            DBSPCompiler compiler, List<DBSPDeclaration> declarations, ObjectNode record) {
        ArrayNode decls = record.putArray("static_decls");
        ArrayNode inits = record.putArray("static_inits");
        for (DBSPDeclaration declaration : declarations) {
            var stat = ((DBSPStaticItem) declaration.item).expression;
            String name = stat.getName();
            String type = ToRustInnerVisitor.toRustString(compiler, stat.getType(), null, false);
            String init = ToRustInnerVisitor.toRustString(compiler, stat.initializer, null, false);
            decls.add("static " + name + ": StaticLazy<" + type + "> = StaticLazy::new();");
            inits.add(name + ".init(move || " + init + ");");
        }
    }

    /** The closure IR plus the STATIC declarations it references, self-contained. */
    private static ObjectNode buildIr(
            DBSPCompiler compiler, DBSPClosureExpression closure, List<DBSPDeclaration> declarations)
            throws Exception {
        ArrayNode irDeclarations = MAPPER.createArrayNode();
        for (DBSPDeclaration declaration : declarations) {
            ObjectNode wrapper = MAPPER.createObjectNode();
            wrapper.set("item", MAPPER.readTree(innerJson(compiler, declaration.item)));
            irDeclarations.add(wrapper);
        }
        ObjectNode ir = MAPPER.createObjectNode();
        ir.set("declarations", irDeclarations);
        ir.set("function", MAPPER.readTree(innerJson(compiler, closure)));
        return ir;
    }

    private static String innerJson(DBSPCompiler compiler, IDBSPInnerNode node) {
        JsonStream stream = new JsonStream(new IndentStreamBuilder());
        ToJsonInnerVisitor visitor = new ToJsonInnerVisitor(compiler, stream, 1);
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

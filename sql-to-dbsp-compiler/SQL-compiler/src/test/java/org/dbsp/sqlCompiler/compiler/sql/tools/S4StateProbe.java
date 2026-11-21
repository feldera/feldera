package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Consumer;

/** Bounded read-only identity/referent observations, never a heap checkpoint.
 * No predicate, transform, getter, iterator callback, or arbitrary toString is
 * invoked. Collection iteration uses only the actual collection implementations.
 * Object references are retained only for this compilation's observation IDs. */
final class S4StateProbe implements DBSPCompiler.StateObserver {
    private final IdentityHashMap<Object, Integer> identities = new IdentityHashMap<>();
    private final ArrayDeque<Integer> stack = new ArrayDeque<>();
    private final Consumer<ObjectNode> sink;
    private int nextEvent;
    private static final int SAMPLE_LIMIT = 32;

    S4StateProbe(Consumer<ObjectNode> sink) { this.sink = sink; }

    private int identity(Object object) {
        Integer id = this.identities.get(object);
        if (id == null) { id = this.identities.size(); this.identities.put(object, id); }
        return id;
    }

    private ObjectNode reference(@Nullable Object value) {
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        if (value == null) return result.put("null", true);
        result.put("class", value.getClass().getName());
        if (value instanceof String string) return result.put("scalar", string);
        if (value instanceof Boolean bool) return result.put("scalar", bool);
        if (value instanceof Number number) {
            // Standard boxed primitive values only; arbitrary Number subclasses
            // are objects, not an invitation to execute conversion methods.
            if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long)
                return result.put("scalar", number.longValue());
            if (number instanceof Float || number instanceof Double)
                return result.put("scalar", number.doubleValue());
        }
        if (value instanceof Character character) return result.put("scalar", character.toString());
        return result.put("object", this.identity(value));
    }

    private ArrayNode fields(Object object, boolean includeDirectReferents) {
        var result = Utilities.deterministicObjectMapper().createArrayNode();
        var declarations = new ArrayList<Field>();
        for (Class<?> type = object.getClass(); type != null && type != Object.class; type = type.getSuperclass()) {
            if (!type.getName().startsWith("org.dbsp.")) break;
            declarations.addAll(Arrays.asList(type.getDeclaredFields()));
        }
        declarations.sort(Comparator.comparing((Field field) -> field.getDeclaringClass().getName())
                .thenComparing(Field::getName));
        for (var field: declarations) {
            var entry = result.addObject().put("declaring_class", field.getDeclaringClass().getName())
                    .put("name", field.getName()).put("declared_type", field.getGenericType().getTypeName())
                    .put("modifiers", Modifier.toString(field.getModifiers()));
            if (Modifier.isStatic(field.getModifiers())) {
                entry.put("status", "static_source_contract_only");
                continue;
            }
            if (!field.trySetAccessible()) {
                entry.put("status", "inaccessible");
                continue;
            }
            final Object value;
            try { value = field.get(object); }
            catch (IllegalAccessException exception) { throw new IllegalStateException(exception); }
            entry.put("status", "observed").set("value", this.reference(value));
            if (value instanceof Map<?, ?> map) {
                entry.put("size", map.size());
                var items = entry.putArray("sample_entries");
                for (var item: map.entrySet()) {
                    if (items.size() == SAMPLE_LIMIT) break;
                    var pair = items.addObject();
                    pair.set("key", this.reference(item.getKey())); pair.set("value", this.reference(item.getValue()));
                }
                entry.put("sample_complete", items.size() == map.size());
            } else if (value instanceof Collection<?> collection) {
                entry.put("size", collection.size());
                var items = entry.putArray("sample_elements");
                for (var item: collection) {
                    if (items.size() == SAMPLE_LIMIT) break;
                    items.add(this.reference(item));
                }
                entry.put("sample_complete", items.size() == collection.size());
            } else if (includeDirectReferents && value != null
                    && value.getClass().getName().startsWith("org.dbsp.")
                    && !(value instanceof DBSPCompiler) && value != this
                    && !value.getClass().getName().startsWith("org.dbsp.sqlCompiler.circuit.")
                    && !value.getClass().getName().startsWith("org.dbsp.sqlCompiler.ir.")) {
                entry.set("direct_referent_fields", this.fields(value, false));
            }
        }
        return result;
    }

    private ObjectNode event(String phase, String container, int index, int iteration,
                             CircuitTransform pass, @Nullable DBSPCircuit circuit, int event) {
        var result = Utilities.deterministicObjectMapper().createObjectNode()
                .put("schema_version", 1).put("phase", phase).put("event", event)
                .put("container", container).put("index", index).put("iteration", iteration)
                .put("pass_class", pass.getClass().getName()).put("pass_object", this.identity(pass))
                .put("sample_limit", SAMPLE_LIMIT).put("max_referent_depth", 1);
        if (!this.stack.isEmpty()) result.put("parent_event", this.stack.peek());
        result.set("circuit", this.reference(circuit));
        result.set("fields", this.fields(pass, true));
        return result;
    }

    @Override public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {
        int event = this.nextEvent++;
        this.sink.accept(this.event("before", container, index, iteration, pass, circuit, event));
        this.stack.push(event);
    }

    @Override public void after(String container, int index, int iteration, CircuitTransform pass,
                                @Nullable DBSPCircuit circuit, boolean completed) {
        int event = this.stack.pop();
        this.sink.accept(this.event("after", container, index, iteration, pass, circuit, event)
                .put("completed", completed));
    }

    @Override public void inPlace(String container, int index, String operation, DBSPCircuit circuit) {
        var entry = Utilities.deterministicObjectMapper().createObjectNode().put("schema_version", 1)
                .put("phase", "in_place").put("container", container).put("index", index).put("operation", operation);
        if (!this.stack.isEmpty()) entry.put("parent_event", this.stack.peek());
        entry.set("circuit", this.reference(circuit)); this.sink.accept(entry);
    }

    ObjectNode ambient(DBSPCompiler compiler, String phase) {
        var record = Utilities.deterministicObjectMapper().createObjectNode().put("phase", phase);
        record.set("compiler", this.reference(compiler)); record.set("fields", this.fields(compiler, true));
        return record;
    }

    static ObjectNode compile(com.fasterxml.jackson.databind.JsonNode source, @Nullable S4StateProbe observer,
                              CorpusDeterminism.Canonicalizer canonicalizer) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        var options = mapper.treeToValue(source.get("options"), org.dbsp.sqlCompiler.compiler.CompilerOptions.class);
        options.ioOptions.dumpProducerCommit = System.getProperty("feldera.s4state.producer", "");
        options.ioOptions.dumpRuntimeAbi = System.getProperty("feldera.s4state.runtime", "");
        org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath.reset();
        var compiler = new DBSPCompiler(options);
        compiler.stateObserver = observer;
        var digest = org.apache.commons.codec.digest.DigestUtils.getSha256Digest();
        int[] observations = {0};
        compiler.circuitObservationHook = json -> {
            try {
                byte[] canonical = canonicalizer.apply(json).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(canonical.length).array());
                digest.update(canonical); observations[0]++;
            } catch (IOException exception) { throw new java.io.UncheckedIOException(exception); }
        };
        if (observer != null) observer.sink.accept(observer.ambient(compiler, "compiler_before"));
        compiler.submitStatementsForCompilation(source.path("program").textValue());
        var circuit = compiler.getFinalCircuit(false);
        if (observer != null) {
            observer.sink.accept(observer.ambient(compiler, "compiler_after"));
            if (!observer.balanced()) throw new IOException("Unbalanced observed pass calls");
        }
        var result = mapper.createObjectNode().put("id", source.path("id").textValue())
                .put("accepted", circuit != null && !compiler.hasErrors())
                .put("observations", observations[0]).put("prefix_sha256", java.util.HexFormat.of().formatHex(digest.digest()));
        result.set("schema", compiler.metadata.asJson());
        result.set("diagnostics", compiler.messages.toJson(compiler.sources));
        if (!result.path("accepted").asBoolean()) throw new IOException("State activation failed: " + result);
        return result;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) throw new IllegalArgumentException("usage: S4StateProbe INPUT OUTPUT CANONICALIZER");
        var mapper = Utilities.deterministicObjectMapper();
        var output = java.nio.file.Path.of(args[1]);
        java.nio.file.Files.createDirectory(output);
        int count = 0;
        try (var canonicalizer = new CorpusDeterminism.Canonicalizer(args[2]);
             var input = java.nio.file.Files.newBufferedReader(java.nio.file.Path.of(args[0]));
             var results = java.nio.file.Files.newBufferedWriter(output.resolve("comparisons.jsonl"))) {
            String line;
            while ((line = input.readLine()) != null) {
                var source = mapper.readTree(line);
                var baseline = compile(source, null, canonicalizer);
                ObjectNode observed;
                try (var compressed = new java.util.zip.GZIPOutputStream(java.nio.file.Files.newOutputStream(
                        output.resolve("state-" + count + ".jsonl.gz")));
                     var writer = new java.io.BufferedWriter(new java.io.OutputStreamWriter(compressed,
                             java.nio.charset.StandardCharsets.UTF_8))) {
                    var collector = new S4StateProbe(record -> {
                        try { writer.write(record.toString()); writer.newLine(); }
                        catch (IOException exception) { throw new java.io.UncheckedIOException(exception); }
                    });
                    observed = compile(source, collector, canonicalizer);
                }
                var comparison = mapper.createObjectNode().put("id", source.path("id").textValue())
                        .put("equal", baseline.equals(observed));
                comparison.set("baseline", baseline); comparison.set("observed", observed);
                results.write(comparison.toString()); results.newLine(); results.flush();
                if (!baseline.equals(observed)) throw new IOException("State observer changed compiler output: " + comparison);
                count++;
                System.err.println("State activation cases: " + count);
            }
        }
        if (count != 32) throw new IOException("Expected the 32 declared option activations, got " + count);
        var complete = mapper.createObjectNode().put("complete", true).put("cases", count)
                .put("all_prefix_observations_equal", true).put("sample_limit", SAMPLE_LIMIT)
                .put("max_referent_depth", 1).put("heap_checkpoint", false).put("runtime_verified", false);
        java.nio.file.Files.writeString(output.resolve("complete.json"), complete.toString());
    }

    boolean balanced() { return this.stack.isEmpty(); }
}

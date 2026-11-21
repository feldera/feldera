package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitOptimizer;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Conditional;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Repeat;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Inventory constructed optimizer paths, including unexecuted conditional bodies.
 * Manual composites still require their explicit source inventory. */
public final class S4PassInventory {
    private S4PassInventory() { }

    static ArrayNode paths(CircuitTransform root) {
        ArrayNode points = Utilities.deterministicObjectMapper().createArrayNode();
        Set<CircuitTransform> active = Collections.newSetFromMap(new IdentityHashMap<>());
        walk(root, Utilities.deterministicObjectMapper().createArrayNode(), points, active);
        return points;
    }

    private static void child(CircuitTransform transform, String container, int index,
                              ArrayNode prefix, ArrayNode points, Set<CircuitTransform> active) {
        ArrayNode path = prefix.deepCopy();
        ObjectNode frame = path.addObject();
        frame.put("container", container);
        frame.put("pass", transform.getName());
        frame.put("index", index);
        ObjectNode point = points.addObject();
        point.set("path", path);
        point.put("implementation", transform.getClass().getName());
        point.put("execution_proven", false);
        walk(transform, path, points, active);
    }

    private static void walk(CircuitTransform transform, ArrayNode prefix,
                             ArrayNode points, Set<CircuitTransform> active) {
        if (!active.add(transform))
            throw new IllegalStateException("Cycle in constructed optimizer transforms");
        try {
            if (transform instanceof Passes passes) {
                for (int index = 0; index < passes.passes.size(); index++)
                    child(passes.passes.get(index), passes.getName(), index, prefix, points, active);
            } else if (transform instanceof Repeat repeat) {
                child(repeat.transform, "Repeat", 0, prefix, points, active);
            } else if (transform instanceof Conditional conditional) {
                // Do not evaluate a metadata predicate outside its active compilation.
                child(conditional.transform, "Conditional", 0, prefix, points, active);
            }
        } finally {
            active.remove(transform);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1)
            throw new IllegalArgumentException("Usage: S4PassInventory OUTPUT.json");
        var mapper = Utilities.deterministicObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("schema_version", 2);
        result.put("field_scope", "all declared fields in each constructed transform class, its superclasses, and interfaces; values and dynamic referents are not inspected");
        result.put("manual_composites_included", true);
        result.set("manual_points", S4ManualPoints.declarations());
        result.put("execution_proven", false);
        ObjectNode implementations = result.putObject("implementations");
        ArrayNode configurations = result.putArray("configurations");
        for (int mask = 0; mask < 16; mask++) {
            var options = new CompilerOptions();
            options.languageOptions.incrementalize = (mask & 1) != 0;
            options.languageOptions.outputsAreSets = (mask & 2) != 0;
            options.languageOptions.generateInputForEveryTable = (mask & 4) != 0;
            options.ioOptions.gen2 = (mask & 8) != 0;
            var compiler = new DBSPCompiler(options);
            ObjectNode configuration = configurations.addObject();
            configuration.set("options", mapper.valueToTree(options));
            var points = paths(new CircuitOptimizer(compiler));
            configuration.set("points", points);
            for (var point: points) {
                String implementation = point.path("implementation").textValue();
                if (!implementations.has(implementation)) {
                    try {
                        var type = Class.forName(implementation, false, S4PassInventory.class.getClassLoader());
                        var contract = implementations.putObject(implementation);
                        contract.set("fields", S4Fields.declarations(type));
                        contract.put("field_lifetimes_reviewed", false);
                    } catch (ClassNotFoundException error) {
                        throw new IOException("Constructed pass class disappeared: " + implementation, error);
                    }
                }
            }
        }
        var ambient = result.putObject("ambient_fields");
        for (var type: java.util.List.of(CircuitOptimizer.class, DBSPCompiler.class, CompilerOptions.class,
                CompilerOptions.Language.class, CompilerOptions.IO.class,
                org.dbsp.sqlCompiler.compiler.ProgramMetadata.class, org.dbsp.sqlCompiler.compiler.ViewOrigins.class,
                org.dbsp.sqlCompiler.compiler.errors.CompilerMessages.class,
                org.dbsp.sqlCompiler.compiler.errors.SourceFileContents.class))
            ambient.set(type.getName(), S4Fields.declarations(type));
        Files.writeString(Path.of(args[0]), mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result),
                StandardOpenOption.CREATE_NEW);
    }
}

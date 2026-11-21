package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;

/** Export the trusted static registry inventory. This is not a callable SQL
 * function catalog or evidence that every operator can cross the lowering seam. */
public final class S1OperatorRegistryProbe {
    private S1OperatorRegistryProbe() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 0) throw new IOException("usage: S1OperatorRegistryProbe");
        var registry = new S1OperatorRegistry();
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("schema_version", 1);
        result.put("registry", S1OperatorRegistry.NAME);
        result.put("registry_version", S1OperatorRegistry.VERSION);
        var entries = result.putArray("entries");
        var identities = new TreeSet<String>();
        for (var entry: new TreeMap<>(registry.entries()).entrySet()) {
            var operator = entry.getValue();
            String canonical = registry.key(operator);
            if (registry.resolve(S1OperatorRegistry.NAME, S1OperatorRegistry.VERSION, canonical) != operator)
                throw new IOException("Operator object identity changed");
            identities.add(canonical);
            var item = entries.addObject();
            item.put("key", entry.getKey());
            item.put("canonical_key", canonical);
            item.put("operator_class", operator.getClass().getName());
            item.put("display_name", operator.getName());
            item.put("kind", operator.getKind().name());
            item.put("syntax", operator.getSyntax().name());
        }
        result.put("keys", entries.size());
        result.put("operator_objects", identities.size());
        result.put("complete", true);
        result.put("runtime_executed", false);
        result.put("S1_parity_proven", false);
        Utilities.deterministicObjectMapper().writerWithDefaultPrettyPrinter().writeValue(System.out, result);
    }
}

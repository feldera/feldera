package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Inventory exact trusted objects from both builtin operator registries. */
public final class S1BuiltinOperatorRegistryProbe {
    private S1BuiltinOperatorRegistryProbe() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) throw new IOException("usage: S1BuiltinOperatorRegistryProbe OUTPUT.json");
        var registry = new S1BuiltinOperatorRegistry();
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("schema_version", 1).put("complete", true);
        var entries = result.putArray("entries");
        var sorted = registry.entries().entrySet().stream().sorted(Comparator.comparing(
                entry -> entry.getKey().registry() + "/" + entry.getKey().key())).toList();
        for (var entry: sorted) {
            if (registry.resolve(entry.getKey()) != entry.getValue()) throw new IOException("Registry object changed");
            var identity = registry.identify(entry.getValue());
            var record = entries.addObject();
            record.put("registry", entry.getKey().registry()).put("version", entry.getKey().version())
                    .put("key", entry.getKey().key()).put("canonical_key", identity.key())
                    .put("class", entry.getValue().getClass().getName()).put("name", entry.getValue().getName());
        }
        result.put("keys", entries.size()).put("S1_parity_proven", false);
        Files.writeString(Path.of(args[0]), result.toPrettyString(), java.nio.file.StandardOpenOption.CREATE_NEW);
    }
}

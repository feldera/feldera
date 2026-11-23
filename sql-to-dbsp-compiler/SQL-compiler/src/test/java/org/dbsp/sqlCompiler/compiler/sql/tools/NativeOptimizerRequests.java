package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.TreeMap;

/** Per-program counts for optimizer operations that can delegate a decision to Java. */
final class NativeOptimizerRequests {
    private static final class Count {
        int calls;
        int fallbacks;
    }

    private final TreeMap<String, Count> operations = new TreeMap<>();

    void record(String operation, boolean answered) {
        var count = this.operations.computeIfAbsent(operation, ignored -> new Count());
        count.calls++;
        if (!answered)
            count.fallbacks++;
    }

    void writeTo(ObjectNode result) {
        this.operations.forEach((name, count) -> result.putObject(name)
                .put("calls", count.calls).put("fallbacks", count.fallbacks));
    }
}

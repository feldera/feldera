package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Execute an explicit command schedule in one JVM. Timings include CLI parsing,
 * compilation, file transport and output writing, but exclude JVM startup and
 * reading this schedule. Every command constructs a fresh compiler instance. */
public final class WarmCompilerBenchmark {
    private WarmCompilerBenchmark() { }

    static String[] arguments(JsonNode command) throws IOException {
        if (!command.isArray() || command.isEmpty()) throw new IOException("Expected nonempty command arguments");
        var arguments = new String[command.size()];
        for (int index = 0; index < arguments.length; index++) {
            if (!command.get(index).isTextual()) throw new IOException("Command argument must be a string");
            arguments[index] = command.get(index).textValue();
        }
        return arguments;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IOException("Usage: WarmCompilerBenchmark SCHEDULE.json RESULTS.jsonl");
        var mapper = Utilities.deterministicObjectMapper();
        var schedule = mapper.readTree(Files.readString(Path.of(args[0])));
        if (!schedule.isArray() || schedule.isEmpty()) throw new IOException("Empty command schedule");
        var memory = ManagementFactory.getMemoryMXBean();
        var collectors = ManagementFactory.getGarbageCollectorMXBeans();
        try (var writer = Files.newBufferedWriter(Path.of(args[1]), StandardOpenOption.CREATE_NEW)) {
            for (int index = 0; index < schedule.size(); index++) {
                var command = arguments(schedule.get(index));
                long beforeHeap = memory.getHeapMemoryUsage().getUsed();
                long beforeGc = collectors.stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum();
                long start = System.nanoTime();
                var messages = CompilerMain.runAndReportErrors(command);
                long elapsed = System.nanoTime() - start;
                long afterGc = collectors.stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum();
                var row = mapper.createObjectNode().put("index", index).put("elapsed_ns", elapsed)
                        .put("exit_code", messages.exitCode).put("heap_before_bytes", beforeHeap)
                        .put("heap_after_bytes", memory.getHeapMemoryUsage().getUsed())
                        .put("gc_collection_ms", afterGc - beforeGc);
                writer.write(row.toString()); writer.newLine(); writer.flush();
                if (messages.exitCode != 0) throw new IOException("Compilation failed at schedule index " + index);
            }
        }
    }
}

package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor.OptimizerSnapshots;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Lineage;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/** Compare --correlatedColumns on the same live circuit in both compilers. */
public final class LineageOracle extends FrozenCircuitObserver {
    private final DBSPCompiler compiler;
    private boolean usedJava;
    private int visits;
    private int nativeReports;
    private int failures;
    private long lines;

    private LineageOracle(DBSPCompiler compiler, ObjectNode row) {
        super(compiler, row);
        this.compiler = compiler;
    }

    private record Outcome(String report, @Nullable String error) {}

    private static Outcome capture(CircuitTransform pass, DBSPCircuit circuit) {
        var buffer = new ByteArrayOutputStream();
        var saved = System.out;
        String error = null;
        try (var stream = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
            System.setOut(stream);
            pass.apply(circuit);
        } catch (RuntimeException ex) {
            error = ex.getClass().getName() + ": " + ex.getMessage();
        } finally {
            System.setOut(saved);
        }
        return new Outcome(buffer.toString(StandardCharsets.UTF_8), error);
    }

    @Override
    public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {
        // This is the first unconditional pass after the optional report.
        if (!pass.getName().equals("DeterministicFunctions"))
            return;
        var direct = capture(new Lineage(this.compiler, true), circuit);
        this.usedJava = false;
        var fallback = new Lineage(this.compiler, true) {
            @Override
            public DBSPCircuit apply(DBSPCircuit input) {
                LineageOracle.this.usedJava = true;
                return super.apply(input);
            }
        };
        var hybrid = capture(new RustCircuitPass(this.compiler, fallback, new OptimizerSnapshots(this.compiler)), circuit);
        if (!direct.equals(hybrid))
            throw new AssertionError(this.observation + "\nJava: " + direct + "\nRust: " + hybrid);
        this.visits++;
        this.lines += direct.report.lines().count();
        if (direct.error != null)
            this.failures++;
        if (!this.usedJava)
            this.nativeReports++;
    }

    @Override
    public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit, boolean done) {}

    @Override
    void finish() {
        if (this.visits == 0)
            throw new AssertionError("No lineage comparison for " + this.observation);
        this.observation.put("visits", this.visits).put("native_reports", this.nativeReports)
                .put("lines", this.lines).put("direct_failures", this.failures);
    }

    public static void main(String[] args) throws IOException {
        run(Path.of(args[0]), LineageOracle::new);
    }
}

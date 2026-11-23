package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;

import java.io.IOException;
import java.nio.file.Path;
import java.util.TreeMap;

/** Count native answers and Java fallbacks on each frozen hybrid compilation. */
public final class OptimizerCoverageOracle extends FrozenCircuitObserver {
    private final NativeOptimizerRequests requests = new NativeOptimizerRequests();
    private final TreeMap<String, TreeMap<String, Integer>> transfers = new TreeMap<>();

    private OptimizerCoverageOracle(DBSPCompiler compiler, ObjectNode observation) {
        super(compiler, observation);
        compiler.options.ioOptions.rustOptimizerPasses = true;
    }

    @Override
    public void nativeDecision(String operation, boolean answered) {
        this.requests.record(operation, answered);
    }

    @Override
    public void monotonicityTransfer(String operator, String rule) {
        this.transfers.computeIfAbsent(operator, ignored -> new TreeMap<>()).merge(rule, 1, Integer::sum);
    }

    @Override
    public void before(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit) {}

    @Override
    public void after(String parent, int index, int iteration, CircuitTransform pass, DBSPCircuit circuit, boolean done) {}

    @Override
    void finish() {
        this.requests.writeTo(this.observation.putObject("requests"));
        var operators = this.observation.putObject("transfers");
        this.transfers.forEach((operator, counts) -> counts.forEach(operators.putObject(operator)::put));
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 2)
            throw new IllegalArgumentException("Usage: OptimizerCoverageOracle POPULATION [S2_DIRECTORY]");
        run(Path.of(args[0]), OptimizerCoverageOracle::new, args.length == 2 ? Path.of(args[1]) : null);
    }
}

package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDeindexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNegateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNoopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamAntiJoinOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.RustCircuitPass;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Compare projection reconstruction at the limiter pass before later optimizations remove the projections. */
public final class LimiterProjectionOracle {
    private static long compile(Path output, boolean hybrid, boolean antiJoin, boolean lateness) {
        var options = new CompilerOptions();
        options.languageOptions.incrementalize = false;
        options.ioOptions.rustOptimizerPasses = hybrid;
        var compiler = new DBSPCompiler(options);
        compiler.optimizerHook = optimizer -> optimizer.insertBefore(MonotoneAnalyzer.class,
                new CircuitCloneVisitor(compiler, false) {
                    @Override
                    public void postorder(DBSPSourceMultisetOperator table) {
                        if (!table.tableName.name().equals("t")) {
                            super.postorder(table);
                            return;
                        }
                        this.addOperator(table);
                        var site = table.getRelNode();
                        var negate = new DBSPNegateOperator(site, table.outputPort());
                        this.addOperator(negate);
                        var noop = new DBSPNoopOperator(site, negate.outputPort());
                        this.addOperator(noop);
                        var row = noop.getOutputZSetElementType().ref().var();
                        var pair = new DBSPRawTupleExpression(
                                new DBSPTupleExpression(row.deref().field(0)), row.deref().applyClone());
                        var index = new DBSPMapIndexOperator(site, pair.closure(row), noop.outputPort());
                        this.addOperator(index);
                        var indexed = new DBSPNoopOperator(site, index.outputPort());
                        this.addOperator(indexed);
                        var port = indexed.outputPort();
                        if (antiJoin) {
                            var join = new DBSPStreamAntiJoinOperator(site, port, index.outputPort());
                            this.addOperator(join);
                            port = join.outputPort();
                        }
                        this.map(table, new DBSPDeindexOperator(site, table.getNode(), port));
                    }
                });
        long[] counter = { -1 };
        compiler.stateObserver = new DBSPCompiler.StateObserver() {
            public void before(String container, int index, int iteration, CircuitTransform pass, DBSPCircuit input) {
                if (pass.getName().equals("InsertLimiters") && (pass instanceof RustCircuitPass) != hybrid)
                    throw new AssertionError("Unexpected limiter implementation");
            }

            public void after(String container, int index, int iteration, CircuitTransform pass,
                              DBSPCircuit circuit, boolean completed) {
                if (!completed || !pass.getName().equals("InsertLimiters"))
                    return;
                counter[0] = DBSPVariablePath.nextGeneratedName();
                var writer = ToJsonOuterVisitor.createForReplay(compiler);
                circuit.accept(writer);
                try {
                    Files.writeString(output, writer.getJsonString());
                } catch (IOException error) {
                    throw new UncheckedIOException(error);
                }
            }
        };
        String column = lateness ? "ts BIGINT NOT NULL LATENESS 1" : "ts BIGINT NOT NULL";
        compiler.submitStatementsForCompilation("CREATE TABLE t(" + column + ", x INT); CREATE VIEW v AS SELECT * FROM t;");
        compiler.getFinalCircuit(false);
        if (counter[0] < 0) {
            compiler.throwIfErrorsOccurred();
            throw new AssertionError("Missing InsertLimiters observation");
        }
        return counter[0];
    }

    /** Arguments are the sql-difftest executable and a fresh output directory. */
    public static void main(String[] args) throws Exception {
        if (args[0].equals("--capture")) {
            long counter = compile(Path.of(args[1]), Boolean.parseBoolean(args[2]),
                    Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]));
            Files.writeString(Path.of(args[1] + ".names"), Long.toString(counter));
            return;
        }
        Path output = Files.createDirectory(Path.of(args[1]));
        for (boolean antiJoin : List.of(false, true)) {
            for (boolean lateness : List.of(false, true)) {
                String name = (antiJoin ? "antijoin" : "unary") + (lateness ? "-late" : "-unbounded");
                Path java = output.resolve(name + "-java.json");
                Path rust = output.resolve(name + "-rust.json");
                // Calcite relation IDs in the checkpoints are process-local. Start the two
                // compilers in fresh JVMs so their input relations have the same identities.
                for (boolean hybrid : List.of(false, true)) {
                    var command = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                            "-ea", "-Djava.library.path=" + System.getProperty("java.library.path"),
                            "-cp", System.getProperty("java.class.path"), LimiterProjectionOracle.class.getName(),
                            "--capture", (hybrid ? rust : java).toString(), Boolean.toString(hybrid),
                            Boolean.toString(antiJoin), Boolean.toString(lateness));
                    if (command.inheritIO().start().waitFor() != 0)
                        throw new AssertionError(name + " failed with rustOptimizerPasses=" + hybrid);
                }
                if (Files.mismatch(Path.of(java + ".names"), Path.of(rust + ".names")) != -1)
                    throw new AssertionError(name + " generated-name counters differ");
                int result = new ProcessBuilder(args[0], java.toString(), rust.toString()).inheritIO().start().waitFor();
                if (result != 0)
                    throw new AssertionError(name + " limiter circuit differs");
            }
        }
    }
}

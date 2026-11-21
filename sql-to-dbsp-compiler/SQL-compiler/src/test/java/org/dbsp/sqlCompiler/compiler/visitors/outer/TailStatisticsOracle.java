package org.dbsp.sqlCompiler.compiler.visitors.outer;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.nio.file.Path;

/** Emit the Java visitor's counters for differential checks of the optimizer tail. */
public final class TailStatisticsOracle {
    private TailStatisticsOracle() {}

    public static JsonNode counters(CircuitStatistics visitor) {
        return Utilities.deterministicObjectMapper().valueToTree(visitor.stats);
    }

    public static void main(String[] paths) throws IOException {
        var mapper = Utilities.deterministicObjectMapper();
        for (String path : paths) {
            var observation = mapper.readTree(Path.of(path).toFile());
            var options = new CompilerOptions();
            options.ioOptions.gen2 = observation.required("options").required("ioOptions").required("gen2").booleanValue();
            var compiler = DBSPCompiler.forCircuitReplay(options);
            var circuit = new JsonDecoder(compiler.metadata).decodeOuter(
                    observation.required("circuit"), DBSPCircuit.class);
            var visitor = new CircuitStatistics(compiler);
            visitor.apply(circuit);
            System.out.println(mapper.writeValueAsString(visitor.stats));
        }
    }
}

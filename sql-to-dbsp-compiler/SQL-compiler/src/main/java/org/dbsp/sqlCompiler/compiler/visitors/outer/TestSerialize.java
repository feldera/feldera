package org.dbsp.sqlCompiler.compiler.visitors.outer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonOuterVisitor;
import org.dbsp.util.Utilities;
import org.locationtech.jts.util.Assert;

/** Tests serialization to Json and back */
@SuppressWarnings("unused")
public class TestSerialize implements CircuitTransform {
    final DBSPCompiler compiler;

    public TestSerialize(DBSPCompiler compiler) {
        this.compiler = compiler;
    }

    @Override
    public String getName() {
        return "TestSerialize";
    }

    @Override
    public DBSPCircuit apply(DBSPCircuit circuit) {
        ToJsonOuterVisitor visitor = ToJsonOuterVisitor.create(compiler, 1);
        visitor.apply(circuit);
        String str = visitor.getJsonString();
        // System.out.println(str);
        try {
            JsonNode node = Utilities.deterministicObjectMapper().readTree(str);
            JsonDecoder decoder = new JsonDecoder(this.compiler.sqlToRelCompiler.typeFactory);
            DBSPCircuit result = decoder.decodeOuter(node, DBSPCircuit.class);
            Assert.equals(circuit.declarations.size(), result.declarations.size());
            Assert.equals(circuit.allOperators.size(), result.allOperators.size());
            return result;
        } catch (JsonProcessingException ex) {
            System.out.println(str);
            throw new RuntimeException(ex);
        }
    }
}

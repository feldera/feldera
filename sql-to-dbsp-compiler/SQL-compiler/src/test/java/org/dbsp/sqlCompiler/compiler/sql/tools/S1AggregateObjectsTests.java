package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.AggregateFunctionDescription;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.SqlUserDefinedAggregationFunction;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class S1AggregateObjectsTests {
    @Test public void declarationRetainsQuotingPositionsAndNonlinearFlag() throws IOException {
        var factory = new S1TypeTable.Factory();
        var integer = factory.createSqlType(SqlTypeName.INTEGER);
        var name = new SqlIdentifier("Sum", new SqlParserPos(1, 1, 1, 3).withQuoting(true));
        var source = S1ExpressionSources.of(new S1ExpressionSources.Source("declaration", "Sum"));
        for (boolean linear: new boolean[]{false, true}) {
            var original = new SqlUserDefinedAggregationFunction(new AggregateFunctionDescription(name, integer,
                    List.of(new RelDataTypeFieldImpl("arg", 0, integer)), linear));
            var wire = S1AggregateObjects.encode(original, Map.of(integer, 0), source, 0);
            var restored = S1AggregateObjects.decode(wire, List.of(integer), source);
            Assert.assertEquals(linear, restored.isLinear());
            Assert.assertSame(integer, restored.description.returnType);
            Assert.assertEquals(name.getParserPosition(), restored.description.name.getParserPosition());
            Assert.assertEquals(original.description.node.toString(), restored.description.node.toString());
            Assert.assertEquals(original.description.position, restored.description.position);
            Assert.assertEquals(wire, S1AggregateObjects.encode(restored, Map.of(integer, 0), source, 0));
            wire.put("linear", "true");
            Assert.assertThrows(IOException.class, () -> S1AggregateObjects.decode(wire, List.of(integer), source));
        }
    }
}

package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.util.TimestampWithTimeZoneString;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.ExpressionBuilder;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPFlatmap;
import org.dbsp.sqlCompiler.ir.expression.DBSPRawTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPTimestampTzLiteral;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestampTz;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.dbsp.util.IdShuffle;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LiveIndexOperandsTests {
    private final ExpressionBuilder b = new ExpressionBuilder();
    private final DBSPCompiler compiler = new DBSPCompiler(new CompilerOptions());
    private final ObjectMapper json = new ObjectMapper();

    private DBSPClosureExpression index(DBSPExpression value) {
        var row = b.refVar(b.tup(b.i32()));
        return new DBSPRawTupleExpression(b.tuple(b.field(row, 0)), b.tuple(value)).closure(row);
    }

    private DBSPFlatmap flatmap(IdShuffle shuffle) {
        var type = b.tup(new DBSPTypeArray(b.i32(), false));
        var row = type.ref().var();
        return new DBSPFlatmap(CalciteObject.EMPTY, type, row.deref().field(0).closure(row),
                List.of(), null, null, shuffle);
    }

    private JsonNode planWithout(String property, DBSPClosureExpression... functions)
            throws Exception {
        var writer = new OptimizerExpressionWriter(this.compiler);
        var roots = this.json.readTree(writer.encode(functions));
        // A partial snapshot must recover the original values before deciding equality.
        var owners = roots.findParents(property);
        Assert.assertFalse(owners.isEmpty());
        owners.forEach(n -> ((ObjectNode) n).remove(property));
        var captures = new AtomicInteger();
        var live = RustCircuitPass.liveOperands(this.compiler, writer);
        byte[] reply = RustCircuitPass.planIndexFields(this.json.writeValueAsBytes(roots), 100,
                this.json.writeValueAsBytes(null), operands -> {
                    captures.incrementAndGet();
                    return live.capture(operands);
                });
        Assert.assertTrue(captures.get() > 0);
        return this.json.readTree(reply);
    }

    @Test
    public void sharedIndexReloadsShuffleOwners() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustSharedIndexes"));
        var shared = new IdShuffle(1);
        var layout = planWithout("shuffleIdentity", index(flatmap(shared)), index(flatmap(shared)),
                index(flatmap(new IdShuffle(1)))).required("layout");
        Assert.assertEquals(this.json.valueToTree(List.of(List.of(0), List.of(0), List.of(1))), layout);
    }

    @Test
    public void reloadedTimestampFailureRetainsTheIndexAllocator() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean("feldera.optimizer.compareRustSharedIndexes"));
        var first = "2020-01-01 00:00:00 GMT+19:00";
        var incoming = "2020-01-01 00:00:00 GMT-20:00";
        var left = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, DBSPTypeTimestampTz.INSTANCE,
                new TimestampWithTimeZoneString(first));
        var right = new DBSPTimestampTzLiteral(CalciteObject.EMPTY, DBSPTypeTimestampTz.INSTANCE,
                new TimestampWithTimeZoneString(incoming));
        var failed = planWithout("runtimeMicros", index(left), index(right));
        Assert.assertEquals(incoming, failed.required("comparison_failure").required("invalidTimestamp").textValue());
        Assert.assertEquals(101, failed.required("next_generated_name").longValue());
    }
}

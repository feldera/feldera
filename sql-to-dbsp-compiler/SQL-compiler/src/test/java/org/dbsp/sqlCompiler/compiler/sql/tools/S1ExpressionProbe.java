package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.IdentityHashMap;
import java.util.List;

/** Fixed fixture oracle for the general expression-fragment reader.
 * This does not supply statement context or establish whole-program S1 parity. */
public final class S1ExpressionProbe {
    private S1ExpressionProbe() { }
    private static final S1OperatorRegistry OPERATORS = new S1OperatorRegistry();
    private static ObjectNode object() { return Utilities.deterministicObjectMapper().createObjectNode(); }

    private record Fixture(List<RelDataType> types, List<RexNode> expressions) { }

    private static Fixture fixture() {
        var factory = new S1TypeTable.Factory();
        var builder = new RexBuilder(factory);
        var integer = factory.createSqlType(SqlTypeName.BIGINT);
        var row = new RelRecordType(StructKind.FULLY_QUALIFIED,
                List.of(new RelDataTypeFieldImpl("X", 0, integer)), false);
        // The equal, separate type is intentional: the importer must not intern it.
        var equalInteger = factory.createSqlType(SqlTypeName.BIGINT);
        RexNode input = builder.makeInputRef(integer, 0);
        RexNode correlation = builder.makeCorrel(row, new CorrelationId(1));
        RexNode field = builder.makeFieldAccess(correlation, 0);
        RexNode literal = builder.makeExactLiteral(new BigDecimal("9223372036854775807"), integer);
        RexNode call = builder.makeCall(integer, SqlStdOperatorTable.PLUS, List.of(input, literal));
        var parameter = new RexLambdaRef(0, "item", integer);
        RexNode lambda = builder.makeLambdaCall(parameter, List.of(parameter));
        return new Fixture(List.of(integer, row, equalInteger),
                List.of(input, correlation, field, literal, call, parameter, lambda));
    }

    private static int typeId(Fixture fixture, RelDataType type) throws IOException {
        for (int i = 0; i < fixture.types().size(); i++)
            if (fixture.types().get(i) == type) return i;
        throw new IOException("Fixture expression has an undeclared type object");
    }

    private static ObjectNode encode(Fixture fixture) throws IOException {
        ObjectNode result = object();
        result.put("schema_version", "expressions-v4");
        result.set("type_table", S1TypeTable.encodeArena(fixture.types(), List.of()));
        ObjectNode object = result.putArray("operator_objects").addObject();
        object.put("kind", "builtin").put("registry", S1OperatorRegistry.NAME);
        object.put("registry_version", S1OperatorRegistry.VERSION);
        object.put("key", OPERATORS.key(SqlStdOperatorTable.PLUS));
        ObjectNode operator = result.putArray("operators").addObject().put("object", 0);
        operator.putArray("parameters").add(0).add(0);
        operator.put("result", 0);
        var bindings = result.putArray("bindings");
        bindings.addObject().put("kind", "input").put("ty", 1);
        bindings.addObject().put("kind", "correlation").put("ty", 1);
        bindings.addObject().put("kind", "lambda_parameter").put("ty", 0);
        var scopes = result.putArray("scopes");
        var outer = scopes.addObject(); outer.putNull("parent"); outer.putArray("bindings").add(0).add(1);
        var inner = scopes.addObject(); inner.put("parent", 0); inner.putArray("bindings").add(2);
        var ids = new IdentityHashMap<RexNode, Integer>();
        for (int i = 0; i < fixture.expressions().size(); i++) ids.put(fixture.expressions().get(i), i);
        var expressions = result.putArray("expressions");
        for (RexNode rex: fixture.expressions()) {
            ObjectNode expr = expressions.addObject();
            expr.put("ty", typeId(fixture, rex.getType()));
            expr.put("scope", rex instanceof RexLambdaRef ? 1 : 0);
            expr.putObject("origin").put("kind", "absent");
            ObjectNode kind = expr.putObject("kind");
            if (rex instanceof RexLambdaRef) {
                kind.put("kind", "lambda_ref").put("binding", 2);
            } else if (rex instanceof RexInputRef input) {
                kind.put("kind", "input_ref").put("binding", 0).put("field", input.getIndex());
            } else if (rex instanceof RexCorrelVariable) {
                kind.put("kind", "correlation").put("binding", 1);
            } else if (rex instanceof RexFieldAccess field) {
                kind.put("kind", "field_access").put("input", ids.get(field.getReferenceExpr())).put("field", field.getField().getIndex());
            } else if (rex instanceof RexLiteral literal) {
                kind.put("kind", "literal");
                ObjectNode value = kind.putObject("literal");
                value.put("ty", typeId(fixture, literal.getType()));
                value.putObject("origin").put("kind", "absent");
                value.set("value", S1LiteralPayload.encode(literal));
            } else if (rex instanceof RexCall call) {
                if (call.getOperator() != SqlStdOperatorTable.PLUS) throw new IOException("Unknown fixture operator");
                kind.put("kind", "call").put("operator", 0);
                var arguments = kind.putArray("arguments");
                for (RexNode operand: call.getOperands()) arguments.add(ids.get(operand));
            } else if (rex instanceof RexLambda lambda) {
                kind.put("kind", "lambda").put("body_scope", 1).put("body", ids.get(lambda.getExpression()));
                var parameters = kind.putArray("parameters");
                for (RexLambdaRef parameter: lambda.getParameters())
                    parameters.addObject().put("declaration", ids.get(parameter)).put("binding", 2).put("name", parameter.getName()).put("index", parameter.getIndex());
            } else {
                throw new IOException("Unsupported fixture expression " + rex.getClass());
            }
        }
        result.putArray("roots").add(4).add(6).add(4);
        return result;
    }

    static ObjectNode fixtureTable() throws IOException {
        return encode(fixture());
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) throw new IOException("usage: S1ExpressionProbe export OUTPUT_DIR | verify INPUT.json OUTPUT_DIR");
        Fixture expected = fixture();
        ObjectNode encoded = encode(expected);
        Path output;
        if (args[0].equals("export") && args.length == 2) {
            output = Path.of(args[1]);
            Files.createDirectory(output);
            Utilities.deterministicObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.resolve("expressions.json").toFile(), encoded);
        } else if (args[0].equals("verify") && args.length == 3) {
            Path input = Path.of(args[1]);
            if (Files.size(input) > 16 * 1024 * 1024) throw new IOException("Expression fixture exceeds 16 MiB");
            JsonNode wire = Utilities.deterministicObjectMapper().readTree(input.toFile());
            if (!wire.equals(Utilities.deterministicObjectMapper().readTree(encoded.toString()))) throw new IOException("Expression fixture changed across transport");
            var decoded = S1ExpressionTable.read(input, new S1BuiltinOperatorRegistry());
            List<RexNode> imported = decoded.expressions();
            if (decoded.types().get(0) == decoded.types().get(2)
                    || !decoded.types().get(0).equals(decoded.types().get(2)))
                throw new IOException("Equal unshared type entries changed");
            if (!imported.equals(expected.expressions())) throw new IOException("Reconstructed Rex expressions differ");
            if (((RexCall) imported.get(4)).getOperands().get(0) != imported.get(0)
                    || ((RexLambda) imported.get(6)).getExpression() != imported.get(5)
                    || ((RexLambda) imported.get(6)).getParameters().get(0) != imported.get(5))
                throw new IOException("Expression or parameter sharing changed");
            output = Path.of(args[2]); Files.createDirectory(output);
        } else { throw new IOException("Unknown expression probe mode"); }
        ObjectNode complete = object();
        complete.put("schema_version", 1).put("complete", true).put("expressions", 7).put("roots", 3)
                .put("fixture_only", true).put("runtime_executed", false).put("S1_parity_proven", false);
        Utilities.deterministicObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.resolve("complete.json").toFile(), complete);
    }
}

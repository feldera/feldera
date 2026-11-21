package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.dbsp.util.Utilities;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/** Opt-in P0 journal of actual executor requests and Feldera temporal folds.
 * Observations never replace evaluation, including refused and failing folds. */
public final class FoldTrace {
    private FoldTrace() { }

    public static RexExecutor wrap(RexExecutor executor) {
        if (System.getProperty("feldera.fold.trace", "").isBlank())
            return executor;
        return (builder, expressions, output) -> {
            int start = output.size();
            executor.reduce(builder, expressions, output);
            record("Janino", expressions, output.subList(start, output.size()));
        };
    }

    static void record(String evaluator, List<? extends RexNode> before, List<? extends RexNode> after) {
        String path = System.getProperty("feldera.fold.trace", "");
        if (path.isBlank())
            return;
        if (before.size() != after.size())
            throw new IllegalStateException("Fold executor changed expression count");
        var mapper = Utilities.deterministicObjectMapper();
        synchronized (FoldTrace.class) {
            try (var output = Files.newBufferedWriter(Path.of(path), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                for (int i = 0; i < before.size(); i++) {
                    ObjectNode record = mapper.createObjectNode();
                    record.put("schema_version", 1);
                    record.put("case", System.getProperty("feldera.fold.case", "unscoped"));
                    record.put("evaluator", evaluator);
                    record.put("changed", !before.get(i).equals(after.get(i)));
                    record.set("expression", expression(before.get(i), 0));
                    record.set("result", expression(after.get(i), 0));
                    output.write(mapper.writeValueAsString(record));
                    output.newLine();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot write fold journal " + path, e);
            }
        }
    }

    static ObjectNode expression(RexNode node, int depth) {
        if (depth > 128)
            throw new IllegalArgumentException("Fold journal expression exceeds depth 128");
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("kind", node.getKind().name());
        result.put("text", node.toString());
        result.set("type", type(node.getType(), 0));
        if (node instanceof RexLiteral literal) {
            Object value = literal.getValue();
            if (value == null) {
                result.putNull("value");
            } else {
                result.put("value_class", value.getClass().getName());
                // getValue() uses legacy Calendar objects for temporal literals
                // and loses sub-millisecond precision. Use the exact Rex value.
                String text = switch (literal.getType().getSqlTypeName()) {
                    case DATE -> literal.getValueAs(DateString.class).toString();
                    case TIME -> literal.getValueAs(TimeString.class).toString();
                    case TIMESTAMP -> literal.getValueAs(TimestampString.class).toString();
                    default -> value instanceof NlsString string ? string.getValue() : value.toString();
                };
                result.put("value", text);
            }
        } else if (node instanceof RexCall call) {
            result.put("operator", call.getOperator().getName());
            result.put("operator_class", call.getOperator().getClass().getName());
            String builtin = JaninoFoldEligibility.builtin(call.getOperator());
            if (builtin != null)
                result.put("builtin", builtin);
            result.put("deterministic", call.getOperator().isDeterministic());
            result.put("dynamic", call.getOperator().isDynamicFunction());
            var arguments = result.putArray("arguments");
            for (RexNode argument: call.getOperands())
                arguments.add(expression(argument, depth + 1));
        }
        return result;
    }

    private static ObjectNode type(org.apache.calcite.rel.type.RelDataType source, int depth) {
        if (depth > 64)
            throw new IllegalArgumentException("Fold type exceeds depth 64");
        var type = Utilities.deterministicObjectMapper().createObjectNode();
        type.put("name", source.getSqlTypeName().name());
        type.put("precision", source.getPrecision());
        type.put("scale", source.getScale());
        type.put("nullable", source.isNullable());
        type.put("full", source.getFullTypeString());
        if (source.getComponentType() != null)
            type.set("component", type(source.getComponentType(), depth + 1));
        if (source.getKeyType() != null)
            type.set("key", type(source.getKeyType(), depth + 1));
        if (source.getValueType() != null)
            type.set("value", type(source.getValueType(), depth + 1));
        return type;
    }
}

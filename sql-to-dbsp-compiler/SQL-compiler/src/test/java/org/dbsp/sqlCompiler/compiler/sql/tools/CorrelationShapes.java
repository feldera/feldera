package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.IdentityHashMap;

/** Structural inventory, not an executable relational format. Unlike the
 * historical trace, this walk follows RexSubQuery relational edges as well as
 * ordinary inputs and records every local expression-bearing field. */
final class CorrelationShapes {
    private CorrelationShapes() { }

    static ObjectNode capture(RelNode root) {
        var mapper = Utilities.deterministicObjectMapper();
        var result = mapper.createObjectNode();
        var nodes = result.putArray("relations");
        var pending = new ArrayList<RelNode>();
        var ids = new IdentityHashMap<RelNode, Integer>();
        java.util.function.ToIntFunction<RelNode> intern = relation -> {
            var node = relation.stripped();
            var id = ids.get(node);
            if (id == null) {
                id = pending.size(); ids.put(node, id); pending.add(node);
            }
            return id;
        };
        result.put("root", intern.applyAsInt(root));
        int correlates = 0;
        int subqueries = 0;
        for (int id = 0; id < pending.size(); id++) {
            var node = pending.get(id);
            var entry = nodes.addObject().put("id", id)
                    .put("kind", node.getRelTypeName())
                    .put("row_type", node.getRowType().getFullTypeString());
            var inputs = entry.putArray("inputs");
            node.getInputs().forEach(input -> inputs.add(intern.applyAsInt(input)));
            if (node instanceof Correlate correlate) {
                correlates++;
                var fields = entry.putObject("correlate")
                        .put("join_type", correlate.getJoinType().name())
                        .put("correlation", correlate.getCorrelationId().getId());
                var required = fields.putArray("required_columns");
                correlate.getRequiredColumns().forEach(required::add);
            }
            var expressions = entry.putArray("expressions");
            var nested = entry.putArray("subqueries");
            S1RelExpressions.forEachRoot(node, expression -> {
                int slot = expressions.size();
                var local = expressions.addObject().put("kind", expression.getKind().name())
                        .put("type", expression.getType().getFullTypeString())
                        .put("text", expression.toString());
                var references = local.putArray("correlations");
                expression.accept(new RexShuttle() {
                    @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                        references.addObject().put("correlation", variable.id.getId())
                                .put("type", variable.getType().getFullTypeString());
                        return variable;
                    }

                    @Override public RexNode visitFieldAccess(RexFieldAccess access) {
                        if (access.getReferenceExpr() instanceof RexCorrelVariable variable) {
                            references.addObject().put("correlation", variable.id.getId())
                                    .put("field", access.getField().getIndex())
                                    .put("type", access.getType().getFullTypeString());
                            return access;
                        }
                        return super.visitFieldAccess(access);
                    }

                    @Override public RexNode visitSubQuery(RexSubQuery query) {
                        nested.addObject().put("expression", slot)
                                .put("kind", query.getKind().name())
                                .put("relation", intern.applyAsInt(query.rel))
                                .put("type", query.getType().getFullTypeString());
                        return super.visitSubQuery(query);
                    }
                });
            }, ignored -> { });
            subqueries += nested.size();
        }
        result.put("correlates", correlates);
        result.put("subqueries", subqueries);
        return result;
    }
}

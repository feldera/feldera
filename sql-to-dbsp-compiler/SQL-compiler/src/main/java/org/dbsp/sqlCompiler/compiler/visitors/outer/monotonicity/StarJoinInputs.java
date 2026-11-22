package org.dbsp.sqlCompiler.compiler.visitors.outer.monotonicity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.circuit.annotation.AlwaysMonotone;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.StarJoinDeltaExpansion;
import org.dbsp.sqlCompiler.compiler.visitors.outer.expansion.StarJoinFilterMapDeltaExpansion;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;

import java.util.Map;

/** The binary join trees and projections produced by expanding one star join. */
final class StarJoinInputs {
    private StarJoinInputs() {}

    private static void unary(InsertLimiters pass, DBSPSimpleOperator op, ObjectNode entry) {
        entry.put("id", op.id).put("input", op.inputs.get(0).operator.id);
        BinaryJoinInputs.projection(pass, op.outputPort(), entry, "output");
    }

    static void capture(InsertLimiters pass, StarJoinDeltaExpansion expansion, ObjectNode entry) {
        var inputs = entry.putArray("integrals");
        for (var integral : expansion.integrators)
            inputs.addObject().put("source", integral.input().operator.id).put("id", integral.id)
                    .put("monotone", integral.annotations.first(AlwaysMonotone.class) != null);
        var trees = entry.putArray("trees");
        for (var tree : expansion.joins) {
            var term = trees.addObject();
            var joins = term.putArray("joins");
            for (var join : tree.joins()) {
                var binary = joins.addObject();
                BinaryJoinInputs.captureTerm(pass, join, binary);
                binary.putArray("inputs").add(join.left().operator.id).add(join.right().operator.id);
            }
            unary(pass, tree.result(), term.putObject("result"));
        }
        var sum = entry.putObject("sum").put("id", expansion.sum.id);
        var order = sum.putArray("inputs");
        for (var input : expansion.sum.inputs)
            order.add(input.operator.id);
        BinaryJoinInputs.projection(pass, expansion.sum.outputPort(), sum, "output");
        if (expansion instanceof StarJoinFilterMapDeltaExpansion filtered) {
            unary(pass, filtered.filter, entry.putObject("filter"));
            if (filtered.map != null)
                unary(pass, filtered.map, entry.putObject("map"));
        }
    }

    static void recordTransfers(InsertLimiters pass, StarJoinDeltaExpansion expansion,
                                Map<Long, IDBSPInnerNode> roots) {
        for (var tree : expansion.joins) {
            for (var join : tree.joins())
                LimiterInputs.recordTransfer(roots, join.id, pass, join);
            LimiterInputs.recordTransfer(roots, tree.result().id, pass, tree.result());
        }
        if (expansion instanceof StarJoinFilterMapDeltaExpansion filtered) {
            LimiterInputs.recordTransfer(roots, filtered.filter.id, pass, filtered.filter);
            if (filtered.map != null)
                LimiterInputs.recordTransfer(roots, filtered.map.id, pass, filtered.map);
        }
    }
}

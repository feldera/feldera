package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Emit a circuit description as JSON.
 * Currently only the dataflow graph is emitted.
 * The verbosity is unused, but in the future it may be used to control the detail. */
public class ToJsonVisitor extends CircuitRewriter {
    final IIndentStream builder;
    final int verbosity;
    final Map<RelNode, Integer> relId;

    static class FindSourcePositions extends InnerRewriteVisitor {
        final Set<SourcePositionRange> positions;

        public FindSourcePositions(DBSPCompiler compiler, Set<SourcePositionRange> positions) {
            super(compiler, false);
            this.positions = positions;
        }

        @Override
        protected void map(IDBSPInnerNode node, IDBSPInnerNode newOp) {
            if (node.is(DBSPExpression.class)) {
                SourcePositionRange positionRange = node.getNode().getPositionRange();
                if (positionRange.isValid()) {
                    this.positions.add(positionRange);
                }
            }
            super.map(node, newOp);
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
        }
    }

    public ToJsonVisitor(DBSPCompiler compiler, IIndentStream builder, int verbosity,
                         Map<RelNode, Integer> id) {
        super(compiler, new FindSourcePositions(compiler, new HashSet<>()), false);
        this.builder = builder;
        this.verbosity = verbosity;
        this.relId = id;
    }

    Integer getRelId(RelNode rel) {
        return Utilities.getExists(this.relId, rel);
    }

    Set<SourcePositionRange> getPositions() {
        return this.transform.to(FindSourcePositions.class).positions;
    }

    void emitPort(OutputPort port) {
        String inputName = port.operator.getCompactName();
        this.builder.append("{ ")
                .appendJsonLabelAndColon("node")
                .append(Utilities.doubleQuote(inputName))
                .append(", ")
                .appendJsonLabelAndColon("output")
                .append(port.outputNumber)
                .append(" }");
    }

    void process(DBSPSimpleOperator operator) {
        String name = operator.getCompactName();
        this.builder.appendJsonLabelAndColon(name)
                .append("{").increase();
        this.builder.appendJsonLabelAndColon("operation")
                .append("\"").append(operator.operation).append("\",").newline();
        this.builder.appendJsonLabelAndColon("inputs")
                .append("[");

        List<OutputPort> inputs = new ArrayList<>(operator.inputs);
        boolean first = true;
        if (!inputs.isEmpty()) {
            this.builder.increase();
            for (var port : inputs) {
                if (!first)
                    this.builder.append(",").newline();
                first = false;
                this.emitPort(port);
            }
            this.builder.decrease().newline();
        }
        this.builder.append("],").newline();

        if (operator.is(DBSPViewDeclarationOperator.class)) {
            var decl = operator.to(DBSPViewDeclarationOperator.class);
            DBSPSimpleOperator source = decl.getCorrespondingView(this.getParent());
            if (source == null) {
                DBSPNestedOperator nested = this.getParent().as(DBSPNestedOperator.class);
                if (nested != null) {
                    int index = nested.outputViews.indexOf(decl.originalViewName());
                    source = nested.internalOutputs.get(index).simpleNode();
                }
            }
            if (source != null) {
                this.builder.appendJsonLabelAndColon("backedges").append("[");
                this.emitPort(source.outputPort());
                this.builder.append("],").newline();
            }
        }

        this.builder.appendJsonLabelAndColon("calcite");
        CalciteRelNode node = operator.getNode().to(CalciteRelNode.class);
        node.asJson(this.builder, this.relId);
        this.builder.append(",").newline();
        this.builder.appendJsonLabelAndColon("positions")
                .append("[");
        var list = Linq.list(this.getPositions());
        List<String> strings = Linq.map(list, p -> p.asJson().toString());
        if (!strings.isEmpty()) {
            this.builder
                    .increase()
                    .join("," + System.lineSeparator(), strings)
                    .decrease()
                    .newline();
        }
        this.builder.append("]").newline();
        this.builder.decrease().append("}");
    }

    void processNested(DBSPNestedOperator nested) {
        super.preorder(nested);
        this.push(nested);
        String name = nested.getCompactName();
        this.builder.appendJsonLabelAndColon(name)
                .append("{").increase();
        this.builder.appendJsonLabelAndColon("operation")
                .append("\"nested\",").newline();
        this.builder.appendJsonLabelAndColon("outputs")
                .append("[").increase();
        boolean first = true;
        for (var out: nested.internalOutputs) {
            if (!first)
                this.builder.append(",").newline();
            first = false;
            this.emitPort(out);
        }
        this.builder.decrease().newline().append("]");
        for (DBSPOperator op : nested.getAllOperators()) {
            this.builder.append(",").newline();
            this.push(op);
            if (op.is(DBSPSimpleOperator.class)) {
                this.getPositions().clear();
                op.accept(this);
                // Calls FindSourcePositions
                this.process(op.to(DBSPSimpleOperator.class));
            } else if (op.is(DBSPNestedOperator.class)) {
                this.processNested(op.to(DBSPNestedOperator.class));
            }
            this.pop(op);
        }
        this.pop(nested);
        super.postorder(nested);
        this.builder.decrease().newline().append("}");
    }

    @Override
    public VisitDecision preorder(DBSPCircuit node) {
        super.preorder(node);
        boolean first = true;
        for (DBSPOperator op : node.allOperators) {
            if (!first)
                this.builder.append(", ");
            first = false;
            if (op.is(DBSPSimpleOperator.class)) {
                this.getPositions().clear();
                // Calls FindSourcePositions
                op.accept(this);
                this.process(op.to(DBSPSimpleOperator.class));
            } else if (op.is(DBSPNestedOperator.class)) {
                this.processNested(op.to(DBSPNestedOperator.class));
            }
        }
        this.builder.newline();
        super.postorder(node);
        return VisitDecision.STOP;
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        this.builder.append("{").increase();
        return super.startVisit(node);
    }

    @Override
    public void endVisit() {
        super.endVisit();
        this.builder.decrease().append("}");
    }
}

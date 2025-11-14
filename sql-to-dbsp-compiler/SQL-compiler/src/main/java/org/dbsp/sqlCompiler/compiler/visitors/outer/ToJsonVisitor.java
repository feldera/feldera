package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.apache.calcite.rel.RelNode;
import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.OperatorHash;
import org.dbsp.sqlCompiler.circuit.operator.DBSPInputMapWithWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRange;
import org.dbsp.sqlCompiler.compiler.errors.SourcePositionRanges;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.HashString;
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
public class ToJsonVisitor extends CircuitVisitor {
    final IIndentStream builder;
    final int verbosity;
    final Map<RelNode, Integer> relId;

    public static class FindSourcePositions extends InnerVisitor {
        private final Set<SourcePositionRange> positions;
        private final boolean reset;

        public FindSourcePositions(DBSPCompiler compiler, boolean reset) {
            super(compiler);
            this.positions = new HashSet<>();
            this.reset = reset;
        }

        @Override
        public void postorder(DBSPExpression expression) {
            SourcePositionRange positionRange = expression.getNode().getPositionRange();
            if (positionRange.isValid())
                this.positions.add(positionRange);
        }

        @Override
        public void postorder(DBSPParameter parameter) {
            SourcePositionRange positionRange = parameter.getNode().getPositionRange();
            if (positionRange.isValid())
                this.positions.add(positionRange);
        }

        @Override
        public void startVisit(IDBSPInnerNode node) {
            super.startVisit(node);
            if (this.reset)
                this.positions.clear();
        }

        public SourcePositionRanges getPositions() {
            return new SourcePositionRanges(this.positions);
        }
    }

    public ToJsonVisitor(DBSPCompiler compiler, IIndentStream builder, int verbosity,
                         Map<RelNode, Integer> id) {
        super(compiler);
        this.builder = builder;
        this.verbosity = verbosity;
        this.relId = id;
    }

    SourcePositionRanges getPositions(DBSPOperator operator) {
        FindSourcePositions positions = new FindSourcePositions(this.compiler, true);
        operator.accept(positions);
        positions.positions.add(operator.getSourcePosition());
        return positions.getPositions();
    }

    void emitPort(OutputPort port) {
        String inputName = port.operator.getCompactName();
        this.builder.append("{ ")
                .appendJsonLabelAndColon("node")
                .append(Utilities.doubleQuote(inputName, false))
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
            this.builder.appendJsonLabelAndColon("table")
                    .append(Utilities.doubleQuote(decl.tableName.toString(), false))
                    .append(",").newline();
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
        } else if (operator.is(DBSPSourceTableOperator.class)) {
            this.builder.appendJsonLabelAndColon("table")
                    .append(Utilities.doubleQuote(operator.to(DBSPSourceTableOperator.class).tableName.toString(), false))
                    .append(",").newline();
        } else if (operator.is(DBSPSinkOperator.class)) {
            this.builder.appendJsonLabelAndColon("view")
                    .append(Utilities.doubleQuote(operator.to(DBSPSinkOperator.class).viewName.toString(), false))
                    .append(",").newline();
        }

        this.builder.appendJsonLabelAndColon("calcite");
        CalciteRelNode node = operator.getNode().to(CalciteRelNode.class);
        node.asJson(this.builder, this.relId);
        this.builder.append(",").newline();
        this.builder.appendJsonLabelAndColon("positions")
                .append("[");
        var list = Linq.list(this.getPositions(operator));
        if (operator.is(DBSPSourceTableOperator.class) || operator.is(DBSPSinkOperator.class)) {
            if (operator.getSourcePosition().isValid())
                list.add(operator.getSourcePosition());
        }
        List<String> strings = Linq.map(list, p -> p.asJson().toString());
        if (!strings.isEmpty()) {
            this.builder
                    .increase()
                    .join("," + System.lineSeparator(), strings)
                    .decrease()
                    .newline();
        }
        this.builder.append("],").newline();
        HashString hash = OperatorHash.getHash(operator, true);
        if (hash != null) {
            this.builder.appendJsonLabelAndColon("persistent_id");
            this.builder.append(Utilities.doubleQuote(hash.toString(), false)).newline();
        }
        this.builder.decrease().append("}");
    }

    void processWithError(DBSPOperatorWithError operator) {
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

        this.builder.appendJsonLabelAndColon("calcite");
        CalciteRelNode node = operator.getNode().to(CalciteRelNode.class);
        node.asJson(this.builder, this.relId);
        this.builder.append(",").newline();
        this.builder.appendJsonLabelAndColon("positions")
                .append("[");
        var list = Linq.list(this.getPositions(operator));
        List<String> strings = Linq.map(list, p -> p.asJson().toString());
        if (!strings.isEmpty()) {
            this.builder
                    .increase()
                    .join("," + System.lineSeparator(), strings)
                    .decrease()
                    .newline();
        }
        this.builder.append("],").newline();
        HashString hash = OperatorHash.getHash(operator, true);
        if (hash != null) {
            this.builder.appendJsonLabelAndColon("persistent_id");
            this.builder.append(Utilities.doubleQuote(hash.toString(), false)).newline();
        }
        this.builder.decrease().append("}");
    }

    void processInputMapWithWaterline(DBSPInputMapWithWaterlineOperator operator) {
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

        this.builder.appendJsonLabelAndColon("calcite");
        CalciteRelNode node = operator.getNode().to(CalciteRelNode.class);
        node.asJson(this.builder, this.relId);
        this.builder.append(",").newline();
        this.builder.appendJsonLabelAndColon("positions")
                .append("[");
        var list = Linq.list(this.getPositions(operator));
        List<String> strings = Linq.map(list, p -> p.asJson().toString());
        if (!strings.isEmpty()) {
            this.builder
                    .increase()
                    .join("," + System.lineSeparator(), strings)
                    .decrease()
                    .newline();
        }
        this.builder.append("],").newline();
        HashString hash = OperatorHash.getHash(operator, true);
        if (hash != null) {
            this.builder.appendJsonLabelAndColon("persistent_id");
            this.builder.append(Utilities.doubleQuote(hash.toString(), false)).newline();
        }
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
            if (out != null)
                this.emitPort(out);
            else
                this.builder.append("null");
        }
        this.builder.decrease().newline().append("]");
        for (DBSPOperator op : nested.getAllOperators()) {
            this.builder.append(",").newline();
            this.push(op);
            if (op.is(DBSPSimpleOperator.class)) {
                op.accept(this);
                // Calls FindSourcePositions
                this.process(op.to(DBSPSimpleOperator.class));
            } else if (op.is(DBSPNestedOperator.class)) {
                this.processNested(op.to(DBSPNestedOperator.class));
            } else {
                this.processWithError(op.to(DBSPOperatorWithError.class));
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
                // Calls FindSourcePositions
                op.accept(this);
                this.process(op.to(DBSPSimpleOperator.class));
            } else if (op.is(DBSPNestedOperator.class)) {
                this.processNested(op.to(DBSPNestedOperator.class));
            } else if (op.is(DBSPOperatorWithError.class)) {
                this.processWithError(op.to(DBSPOperatorWithError.class));
            } else if (op.is(DBSPInputMapWithWaterlineOperator.class)) {
                this.processInputMapWithWaterline(op.to(DBSPInputMapWithWaterlineOperator.class));
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

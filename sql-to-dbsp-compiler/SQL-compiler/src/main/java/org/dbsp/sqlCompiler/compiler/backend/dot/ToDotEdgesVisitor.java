package org.dbsp.sqlCompiler.compiler.backend.dot;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperatorWithError;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IWritesLogs;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import java.util.HashSet;
import java.util.Set;

/** This visitor dumps the edges circuit to a dot file. */
public class ToDotEdgesVisitor extends CircuitVisitor implements IWritesLogs {
    protected final IndentStream stream;
    // A higher value -> more details
    protected final int details;
    protected final Set<OutputPort> edgesLabeled;

    public ToDotEdgesVisitor(DBSPCompiler compiler, IndentStream stream, int details) {
        super(compiler);
        this.stream = stream;
        this.details = details;
        this.edgesLabeled = new HashSet<>();
    }

    public String getEdgeLabel(OutputPort source) {
        DBSPType type = source.getOutputRowType();
        String name = source.node().getCompactName();
        if (source.port() != 0)
            name += " " + source.port();
        return name + " " +
                ToRustInnerVisitor.toRustString(this.compiler(), type, null, this.details < 3);
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        return super.startVisit(node);
    }

    String getPortName(OutputPort port) {
        String name = port.getName(false);
        // The following is used for operators with multiple outputs
        return name.replace("_", ":p");
    }

    @Override
    public VisitDecision preorder(DBSPOperatorWithError node) {
        for (OutputPort i : node.inputs) {
            DBSPOperator input = i.node();
            this.stream.append(this.getPortName(i))
                    .append(" -> ")
                    .append(node.getNodeName(false));
            if (this.details >= 2 && !this.edgesLabeled.contains(i)) {
                String label = this.getEdgeLabel(i);
                this.stream.append(" [xlabel=")
                        .append(Utilities.doubleQuote(label))
                        .append("]");
                this.edgesLabeled.add(i);
            }
            this.stream.append(";")
                    .newline();
        }
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator node) {
        for (OutputPort i : node.inputs) {
            DBSPOperator input = i.node();
            this.stream.append(this.getPortName(i))
                    .append(" -> ")
                    .append(node.getOutputName());
            if (this.details >= 2 && !this.edgesLabeled.contains(i)) {
                String label = this.getEdgeLabel(i);
                this.stream.append(" [xlabel=")
                        .append(Utilities.doubleQuote(label))
                        .append("]");
                this.edgesLabeled.add(i);
            }
            this.stream.append(";")
                    .newline();
        }

        // Add the back-edge represented implicitly between a View and a ViewDeclaration
        if (node.is(DBSPViewDeclarationOperator.class)) {
            var decl = node.to(DBSPViewDeclarationOperator.class);
            DBSPSimpleOperator source = decl.getCorrespondingView(this.getParent());
            if (source == null) {
                // May happen after views have been deleted
                DBSPNestedOperator nested = this.getParent().as(DBSPNestedOperator.class);
                if (nested != null) {
                    int index = nested.outputViews.indexOf(decl.originalViewName());
                    source = nested.internalOutputs.get(index).simpleNode();
                }
            }
            if (source != null) {
                this.stream.append(source.getOutputName())
                        .append(" -> ")
                        .append(node.getOutputName())
                        .append(";")
                        .newline();
            }
        }
        return VisitDecision.STOP;
    }

    public interface VisitorConstructor {
        CircuitVisitor create(IndentStream stream);
    }
}

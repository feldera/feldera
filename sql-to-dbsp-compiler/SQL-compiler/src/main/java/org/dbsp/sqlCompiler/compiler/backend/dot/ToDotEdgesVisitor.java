package org.dbsp.sqlCompiler.compiler.backend.dot;

import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewOperator;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
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
    protected final Set<DBSPOperator> edgesLabeled;

    public ToDotEdgesVisitor(IErrorReporter reporter, IndentStream stream, int details) {
        super(reporter);
        this.stream = stream;
        this.details = details;
        this.edgesLabeled = new HashSet<>();
    }

    public String getEdgeLabel(OutputPort source) {
        DBSPType type = source.getOutputRowType();
        return ToRustInnerVisitor.toRustString(
                this.errorReporter, type, CompilerOptions.getDefault(), true);
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        return super.startVisit(node);
    }

    @Override
    public VisitDecision preorder(DBSPSimpleOperator node) {
        for (OutputPort i : node.inputs) {
            DBSPOperator input = i.node();
            this.stream.append(input.getOutputName(i.outputNumber))
                    .append(" -> ")
                    .append(node.getOutputName());
            if (this.details >= 2 && !this.edgesLabeled.contains(input)) {
                String label = this.getEdgeLabel(i);
                this.stream.append(" [xlabel=")
                        .append(Utilities.doubleQuote(label))
                        .append("]");
                this.edgesLabeled.add(input);
            }
            this.stream.append(";")
                    .newline();
        }

        // Add the back-edge represented implicitly between a View and a ViewDeclaration
        if (node.is(DBSPViewDeclarationOperator.class)) {
            DBSPViewOperator view = node.to(DBSPViewDeclarationOperator.class)
                    .getCorrespondingView(this.getParent());
            if (view != null) {
                // Should be always true, but since this tool is used for debugging bugs in the compiler,
                // we want to continue even if it's not
                this.stream.append(view.getOutputName())
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

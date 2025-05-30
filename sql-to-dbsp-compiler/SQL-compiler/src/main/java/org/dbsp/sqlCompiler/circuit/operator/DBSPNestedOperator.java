package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** An operator which contains multiple other operators. */
public class DBSPNestedOperator extends DBSPOperator implements ICircuit {
    final List<DBSPOperator> allOperators;
    final Set<DBSPOperator> operators;
    final Map<ProgramIdentifier, DBSPViewOperator> viewByName;
    /** Indexed by original view name */
    public final Map<ProgramIdentifier, DBSPViewDeclarationOperator> declarationByName;
    public final List<DBSPDeltaOperator> deltaInputs;
    /** For each output port of this, the actual port of an operator inside,
     * which produces the result.  Some of these may become null if they are found to be dead. */
    public final List<OutputPort> internalOutputs;
    /** Outputs correspond to views (recursive or not).  Names of these views in order. */
    public final List<ProgramIdentifier> outputViews;

    public DBSPNestedOperator(CalciteRelNode node) {
        super(node);
        this.allOperators = new ArrayList<>();
        this.viewByName = new HashMap<>();
        this.deltaInputs = new ArrayList<>();
        this.internalOutputs = new ArrayList<>();
        this.operators = new HashSet<>();
        this.outputViews = new ArrayList<>();
        this.declarationByName = new HashMap<>();
    }

    @Override
    public boolean hasOutput(int outputNumber) {
        if (outputNumber < 0 || outputNumber >= this.internalOutputs.size())
            return false;
        return this.internalOutputs.get(outputNumber) != null;
    }

    public boolean contains(DBSPOperator operator) {
        return this.operators.contains(operator);
    }

    @Override
    public void addInput(OutputPort port) {
        super.addInput(port);
        Utilities.enforce(!this.contains(port.node()));
    }

    @Override
    public void addOperator(DBSPOperator operator) {
        this.allOperators.add(operator);
        this.operators.add(operator);
        if (operator.is(DBSPViewOperator.class)) {
            DBSPViewOperator view = operator.to(DBSPViewOperator.class);
            Utilities.putNew(this.viewByName, view.viewName, view);
        } else if (operator.is(DBSPDeltaOperator.class)) {
            DBSPDeltaOperator delta = operator.to(DBSPDeltaOperator.class);
            this.deltaInputs.add(delta);
            this.addInput(delta.input());
        } else if (operator.is(DBSPViewDeclarationOperator.class)) {
            var decl = operator.to(DBSPViewDeclarationOperator.class);
            Utilities.putNew(this.declarationByName, decl.originalViewName(), decl);
        }
    }

    /** Add an output for this nested operator.  The port may be null if the corresponding output
     * has actually been deleted.
     * @param view  View corresponding to output.
     * @param port  Output port corresponding to the view (may be in a differentiator).
     * @return      A port of this operator that corresponds
     */
    public OutputPort addOutput(ProgramIdentifier view, @Nullable OutputPort port) {
        this.outputViews.add(view);
        this.internalOutputs.add(port);
        Utilities.enforce(port == null || this.operators.contains(port.node()));
        return new OutputPort(this, this.internalOutputs.size() - 1);
    }

    /** The output of the operator that corresponds to this declaration */
    @Nullable
    public OutputPort outputForDeclaration(DBSPViewDeclarationOperator operator) {
        DBSPSimpleOperator source = operator.getCorrespondingView(this);
        if (source != null)
            return source.outputPort();
        // May happen after views have been deleted
        int index = this.outputViews.indexOf(operator.originalViewName());
        return this.internalOutputs.get(index);
    }

    @Nullable
    @Override
    public DBSPViewOperator getView(ProgramIdentifier name) {
        return this.viewByName.get(name);
    }

    @Override
    public Iterable<DBSPOperator> getAllOperators() {
        return this.allOperators;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop()) {
            visitor.startArrayProperty("allOperators");
            int index = 0;
            for (DBSPOperator op : this.allOperators) {
                visitor.propertyIndex(index++);
                op.accept(visitor);
            }
            visitor.endArrayProperty("allOperators");
            visitor.postorder(this);
        }
        visitor.pop(this);
    }

    @Override
    public void accept(InnerVisitor visitor) {}

    @Override
    public boolean equivalent(DBSPOperator other) {
        if (this == other)
            return true;
        if (other instanceof DBSPNestedOperator sub) {
            if (this.allOperators.size() != sub.allOperators.size())
                return false;
            return Linq.all(Linq.zipSameLength(this.allOperators, sub.allOperators, DBSPOperator::equivalent));
        }
        return false;
    }

    @Override
    public DBSPType outputType(int outputNo) {
        OutputPort port = this.internalOutputs.get(outputNo);
        if (port == null)
            throw new RuntimeException("Getting type from port that does not exist");
        return port.outputType();
    }

    @Override
    public boolean isMultiset(int outputNumber) {
        return this.internalOutputs.get(outputNumber).isMultiset();
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append("nested_")
                .append(this.id)
                .append(" {")
                .increase()
                .append(this.allOperators.size())
                .append(" operators...")
                .newline().decrease().append("}");
    }

    @Override
    public boolean sameCircuit(ICircuit other) {
        if (this == other)
            return true;
        if (other.is(DBSPNestedOperator.class)) {
            return Linq.same(this.allOperators, other.to(DBSPNestedOperator.class).allOperators);
        }
        return false;
    }

    @Override
    public int outputCount() {
        return this.internalOutputs.size();
    }

    @SuppressWarnings("unused")
    public static DBSPNestedOperator fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPNestedOperator result = new DBSPNestedOperator(CalciteEmptyRel.INSTANCE);
        List<DBSPOperator> operators =
                fromJsonOuterList(node, "allOperators", decoder, DBSPOperator.class);
        for (DBSPOperator op : operators)
            result.addOperator(op);
        List<OutputPort> internalOutputs = Linq.list(Linq.map(
                Utilities.getProperty(node, "internalOutputs").elements(),
                e -> OutputPort.fromJson(e, decoder)));
        List<ProgramIdentifier> outputViews = Linq.list(Linq.map(
                Utilities.getProperty(node, "outputViews").elements(),
                ProgramIdentifier::fromJson));
        Utilities.enforce(internalOutputs.size() == outputViews.size());
        for (int i = 0; i < internalOutputs.size(); i++) {
            result.addOutput(outputViews.get(i), internalOutputs.get(i));
        }
        Annotations annotations = Annotations.fromJson(Utilities.getProperty(node, "annotations"));
        return result.addAnnotations(annotations, DBSPNestedOperator.class);
    }
}

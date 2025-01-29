package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeVoid;
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

    public DBSPNestedOperator(CalciteObject node) {
        super(node);
        this.allOperators = new ArrayList<>();
        this.viewByName = new HashMap<>();
        this.deltaInputs = new ArrayList<>();
        this.internalOutputs = new ArrayList<>();
        this.operators = new HashSet<>();
        this.outputViews = new ArrayList<>();
        this.declarationByName = new HashMap<>();
    }

    public boolean contains(DBSPOperator operator) {
        return this.operators.contains(operator);
    }

    @Override
    public void addInput(OutputPort port) {
        super.addInput(port);
        assert !this.contains(port.node());
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
        assert port == null || this.operators.contains(port.node());
        return new OutputPort(this, this.internalOutputs.size() - 1);
    }

    @Override
    public void addDeclaration(DBSPDeclaration declaration) {
        throw new InternalCompilerError("Adding declaration to CircuitOperator");
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
            for (DBSPOperator op : this.allOperators)
                op.accept(visitor);
            visitor.postorder(this);
        }
        visitor.pop(this);
    }

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
        return this.internalOutputs.get(outputNo).outputType();
    }

    @Override
    public boolean isMultiset(int outputNumber) {
        return this.internalOutputs.get(outputNumber).isMultiset();
    }

    @Override
    public String getOutputName(int outputNo) {
        OutputPort port = this.internalOutputs.get(outputNo);
        if (port == null)
            return "_";
        return port.getOutputName();
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

    @Override
    public DBSPType streamType(int outputNumber) {
        OutputPort port = this.internalOutputs.get(outputNumber);
        if (port == null)
            return DBSPTypeVoid.INSTANCE;
        return port.node().streamType(port.outputNumber);
    }
}

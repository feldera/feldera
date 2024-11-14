package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

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
    final Map<String, DBSPViewOperator> viewByName;
    public final List<DBSPViewDeclarationOperator> viewDeclarations;
    final List<DBSPDeltaOperator> deltaInputs;
    /** For each output port of this, the actual port of an operator inside,
     * which produces the result. */
    public final List<OutputPort> outputs;

    public DBSPNestedOperator(CalciteObject node) {
        super(node);
        this.allOperators = new ArrayList<>();
        this.viewByName = new HashMap<>();
        this.deltaInputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
        this.operators = new HashSet<>();
        this.viewDeclarations = new ArrayList<>();
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
            this.viewDeclarations.add(operator.to(DBSPViewDeclarationOperator.class));
        }
    }

    public OutputPort addOutput(OutputPort port) {
        this.outputs.add(port);
        assert this.operators.contains(port.node());
        return new OutputPort(this, this.outputs.size() - 1);
    }

    @Override
    public void addDeclaration(DBSPDeclaration declaration) {
        throw new InternalCompilerError("Adding declaration to CircuitOperator");
    }

    @Override
    public DBSPViewOperator getView(String name) {
        return Utilities.getExists(this.viewByName, name);
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
            return Linq.all(Linq.zipSameLength(this.allOperators, sub.allOperators, DBSPOperator::equivalent));
        }
        return false;
    }

    @Override
    public DBSPType outputType(int outputNo) {
        return this.outputs.get(outputNo).outputType();
    }

    @Override
    public boolean isMultiset(int outputNumber) {
        return this.outputs.get(outputNumber).isMultiset();
    }

    @Override
    public String getOutputName(int outputNo) {
        return this.outputs.get(outputNo).getOutputName();
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
        return this.outputs.size();
    }

    @Override
    public DBSPType streamType(int outputNumber) {
        OutputPort port = this.outputs.get(outputNumber);
        return port.node().streamType(port.outputNumber);
    }
}

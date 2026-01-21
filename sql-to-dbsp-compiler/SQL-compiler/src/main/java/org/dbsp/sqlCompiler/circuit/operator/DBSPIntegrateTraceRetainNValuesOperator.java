package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteRelNode;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.IMaybeMonotoneType;
import org.dbsp.sqlCompiler.compiler.visitors.monotone.PartiallyMonotoneTuple;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Represents multiple DBSP operators, all whose names start with
 * accumulate_integrate_trace_retain_values.  The name end with
 * - last_n
 * - first_n
 * - bottom_n
 * Used to GC the right input of ASOF JOIN operators, Min, Max, ArgMin, ArgMax, MinSome, TopK. */
public final class DBSPIntegrateTraceRetainNValuesOperator
        extends DBSPBinaryOperator implements GCOperator {
    public enum WhichN {
        LastN("last_n"),
        TopN("top_n"),
        BottomN("bottom_n");

        public final String which;

        WhichN(String which) {
            this.which = which;
        }
    }

    public final int n;
    public final WhichN which;

    public DBSPIntegrateTraceRetainNValuesOperator(
            CalciteRelNode node, DBSPExpression function,
            OutputPort data, OutputPort control, int n, WhichN which) {
        super(node, "accumulate_integrate_trace_retain_values_" + which.which,
                function, data.outputType(), data.isMultiset(), data, control, false);
        Utilities.enforce(n > 0);
        this.n = n;
        this.which = which;
    }

    public static DBSPIntegrateTraceRetainNValuesOperator create(
            CalciteRelNode node, OutputPort data, IMaybeMonotoneType dataProjection, OutputPort control,
            int n, WhichN which) {
        DBSPType controlType = control.outputType();
        Utilities.enforce(controlType.is(DBSPTypeTupleBase.class),
                () -> "Control type is not a tuple: " + controlType);
        DBSPTypeTupleBase controlTuple = controlType.to(DBSPTypeTupleBase.class);
        Utilities.enforce(controlTuple.size() == 2);

        DBSPVariablePath controlArg = controlType.ref().var();
        Utilities.enforce(data.outputType().is(DBSPTypeIndexedZSet.class),
                () -> "Data is not indexed: " + data.outputType());
        DBSPType valueType = data.getOutputIndexedZSetType().elementType;
        DBSPVariablePath dataArg = valueType.ref().var();
        DBSPParameter param = new DBSPParameter(dataArg.variable, dataArg.getType());
        DBSPExpression project = dataProjection
                .to(PartiallyMonotoneTuple.class)
                .getField(1)
                .projectExpression(dataArg.deref());
        DBSPExpression compare0 = controlArg.deref().field(0).not();
        DBSPExpression compare = DBSPControlledKeyFilterOperator.generateTupleCompare(
                project, controlArg.deref().field(1), DBSPOpcode.CONTROLLED_FILTER_GTE);
        compare = ExpressionCompiler.makeBinaryExpression(
                node, compare.getType(), DBSPOpcode.OR, compare0, compare);
        DBSPExpression closure = compare.closure(param, controlArg.asParameter());
        return new DBSPIntegrateTraceRetainNValuesOperator(node, closure, data, control, n, which);
    }

    @Override
    public DBSPSimpleOperator with(
            @Nullable DBSPExpression function, DBSPType outputType,
            List<OutputPort> newInputs, boolean force) {
        if (this.mustReplace(force, function, newInputs, outputType)) {
            Utilities.enforce(newInputs.size() == 2, () -> "Expected 2 inputs, got " + newInputs.size());
            return new DBSPIntegrateTraceRetainNValuesOperator(
                    this.getRelNode(), Objects.requireNonNull(function),
                    newInputs.get(0), newInputs.get(1), this.n, this.which);
        }
        return this;
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    // equivalent inherited from parent

    @SuppressWarnings("unused")
    public static DBSPIntegrateTraceRetainNValuesOperator fromJson(JsonNode node, JsonDecoder decoder) {
        CommonInfo info = commonInfoFromJson(node, decoder);
        boolean accumulate = Utilities.getBooleanProperty(node, "accumulate");
        int n = Utilities.getIntProperty(node, "n");
        WhichN which = WhichN.valueOf(Utilities.getStringProperty(node, "which"));

        return new DBSPIntegrateTraceRetainNValuesOperator(CalciteEmptyRel.INSTANCE,
                info.getFunction(), info.getInput(0), info.getInput(1), n, which)
                .addAnnotations(info.annotations(), DBSPIntegrateTraceRetainNValuesOperator.class);
    }

    @Override
    public DBSPSimpleOperator asOperator() {
        return this;
    }
}

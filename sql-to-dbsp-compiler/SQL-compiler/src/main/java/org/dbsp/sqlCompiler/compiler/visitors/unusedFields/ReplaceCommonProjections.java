package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.IsProjection;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceMultisetOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteEmptyRel;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitCloneVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplaceCommonProjections extends CircuitCloneVisitor {
    final FindCommonProjections fcp;
    /** For each operator which is replaced with a narrower one, the replacement.
     * We cannot use the {@link CircuitCloneVisitor#map} function, because that one requires the same output type. */
    final Map<DBSPOperator, OutputPort> narrowed;

    public ReplaceCommonProjections(DBSPCompiler compiler, FindCommonProjections fcp) {
        super(compiler, false);
        this.fcp = fcp;
        this.narrowed = new HashMap<>();
    }

    public boolean process(DBSPSimpleOperator operator) {
        if (this.fcp.outputProjection.containsKey(operator)) {
            List<OutputPort> sources = Linq.map(operator.inputs, this::mapped);
            DBSPClosureExpression projection = this.fcp.outputProjection.get(operator);
            DBSPSimpleOperator replace = operator.withInputs(sources, false);
            this.addOperator(replace);
            int size = operator.outputType().getToplevelFieldCount();
            boolean isRaw = projection.getResultType().is(DBSPTypeRawTuple.class);
            DBSPSimpleOperator result;
            if (isRaw) {
                result = new DBSPMapIndexOperator(
                        CalciteEmptyRel.INSTANCE, projection, replace.outputPort())
                        .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            } else {
                result = new DBSPMapOperator(
                        CalciteEmptyRel.INSTANCE, projection, replace.outputPort())
                        .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            }
            this.addOperator(result);
            Utilities.putNew(this.narrowed, operator, result.outputPort());
            return true;
        }
        return false;
    }

    @Override
    public void postorder(DBSPStreamJoinOperator operator) {
        if (!this.process(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSourceMultisetOperator operator) {
        if (!this.process(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPSourceMapOperator operator) {
        if (!this.process(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        if (!this.process(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        if (this.fcp.inputProjection.containsKey(operator) &&
                // If we forgot to call process(input) the source won't be there
            this.narrowed.containsKey(operator.input().node())) {
            OutputPort source = Utilities.getExists(this.narrowed, operator.input().node());
            DBSPClosureExpression projection = this.fcp.inputProjection.get(operator);
            int size = source.outputType().getToplevelFieldCount();
            DBSPSimpleOperator result = new DBSPMapOperator(
                    operator.getRelNode(), projection, source)
                    .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            this.map(operator, result);
            return;
        }
        if (!this.process(operator))
            super.postorder(operator);
    }

    @Override
    public void postorder(DBSPMapIndexOperator operator) {
        if (this.fcp.inputProjection.containsKey(operator) &&
                // If we forgot to call process(input) the source won't be there
                this.narrowed.containsKey(operator.input().node())) {
            OutputPort source = Utilities.getExists(this.narrowed, operator.input().node());
            DBSPClosureExpression projection = this.fcp.inputProjection.get(operator);
            int size = source.outputType().getToplevelFieldCount();
            DBSPSimpleOperator result = new DBSPMapIndexOperator(
                    operator.getRelNode(), projection, source)
                    .addAnnotation(new IsProjection(size), DBSPSimpleOperator.class);
            this.map(operator, result);
            return;
        }
        if (!this.process(operator))
            super.postorder(operator);
    }
}

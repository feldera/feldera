package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFilterOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPIfExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

/** Combine a map followed by a filter or vice versa into a flatmap. */
public class FilterMapVisitor extends CircuitCloneWithGraphsVisitor {
    public FilterMapVisitor(DBSPCompiler compiler, CircuitGraphs graphs) {
        super(compiler, graphs, false);
    }

    @Override
    public void postorder(DBSPMapOperator operator) {
        DBSPOperator in = this.mapped(operator.input()).node();
        if (in.is(DBSPFilterOperator.class) &&
                (this.getGraph().getFanout(operator.input().node()) == 1)) {
            // Generate code for the function
            // if (filter(input)) {
            //   Some(map(input))
            // } else {
            //   None
            // }
            DBSPFilterOperator source = in.to(DBSPFilterOperator.class);
            DBSPClosureExpression map = operator.getClosureFunction();
            DBSPClosureExpression filter = source.getClosureFunction();
            assert filter.parameters.length == 1;
            DBSPParameter param = filter.parameters[0];

            DBSPVariablePath newParam = param.getType().var();
            DBSPType resultType = map.getResultType();
            DBSPExpression cond = filter.call(newParam);
            DBSPExpression ifTrue = map.call(newParam);
            DBSPIfExpression ifExp = new DBSPIfExpression(
                    operator.getNode(),
                    cond,
                    ifTrue.some(),
                    resultType.withMayBeNull(true).none());
            DBSPClosureExpression function = ifExp.closure(newParam);
            DBSPSimpleOperator result =
                    new DBSPFlatMapOperator(source.getNode(),
                            function, operator.getOutputZSetType(),
                            operator.isMultiset, source.inputs.get(0));
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }

    @Override
    public void postorder(DBSPFilterOperator operator) {
        DBSPOperator in = this.mapped(operator.input()).node();
        if (in.is(DBSPMapOperator.class) &&
                (this.getGraph().getFanout(operator.input().node()) == 1)) {
            // Generate code for the function
            // let tmp = map(...);
            // if (filter(tmp)) {
            //   Some(tmp)
            // } else {
            //   None
            // }
            DBSPMapOperator source = in.to(DBSPMapOperator.class);
            DBSPClosureExpression map = source.getClosureFunction();
            DBSPClosureExpression filter = operator.getClosureFunction();
            DBSPLetStatement let = new DBSPLetStatement("tmp", map.body);
            DBSPExpression cond = filter.call(let.getVarReference().borrow()).reduce(this.compiler());
            DBSPExpression tmp = let.getVarReference();
            DBSPIfExpression ifexp = new DBSPIfExpression(
                    operator.getNode(),
                    cond,
                    tmp.some(),
                    tmp.getType().withMayBeNull(true).none());
            DBSPBlockExpression block = new DBSPBlockExpression(Linq.list(let), ifexp);
            DBSPClosureExpression function = block.closure(map.parameters);
            DBSPSimpleOperator result =
                    new DBSPFlatMapOperator(source.getNode(),
                            function, source.getOutputZSetType(),
                            operator.isMultiset, source.inputs.get(0));
            this.map(operator, result);
            return;
        }
        super.postorder(operator);
    }
}

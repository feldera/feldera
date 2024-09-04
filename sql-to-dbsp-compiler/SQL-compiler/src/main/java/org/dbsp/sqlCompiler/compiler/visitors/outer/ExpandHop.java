package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPFlatMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPHopOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.TypeCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPBlockExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeVec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** Expand HopOperator into a map followed by a flat_map. */
public class ExpandHop extends CircuitCloneVisitor {
    public ExpandHop(IErrorReporter reporter) {
        super(reporter, false);
    }

    @Override
    public void postorder(DBSPHopOperator operator) {
        DBSPOperator source = this.mapped(operator.input());
        CalciteObject node = operator.getNode();

        DBSPTypeTuple type = operator.getOutputZSetElementType().to(DBSPTypeTuple.class);
        DBSPTypeTuple inputRowType = source.getOutputZSetElementType().to(DBSPTypeTuple.class);
        DBSPVariablePath row = inputRowType.ref().var();
        int timestampIndex = operator.timestampIndex;
        DBSPExpression interval = operator.interval;
        @Nullable DBSPExpression start = operator.start;
        DBSPExpression size = operator.size;

        DBSPExpression[] results = new DBSPExpression[inputRowType.size() + 1];
        for (int i = 0; i < inputRowType.size(); i++) {
            results[i] = row.deref().field(i).applyCloneIfNeeded();
        }
        int nextIndex = inputRowType.size();

        // Map builds the array
        List<DBSPExpression> hopArguments = new ArrayList<>();
        hopArguments.add(row.deref().field(timestampIndex));
        hopArguments.add(interval);
        hopArguments.add(size);
        hopArguments.add(start);
        DBSPType hopType = type.tupFields[nextIndex];
        results[nextIndex] = ExpressionCompiler.compilePolymorphicFunction(
                "hop", node, new DBSPTypeVec(hopType, false), hopArguments, 4);
        DBSPTupleExpression mapBody = new DBSPTupleExpression(results);
        DBSPClosureExpression func = mapBody.closure(row.asParameter());
        DBSPMapOperator map = new DBSPMapOperator(node, func, TypeCompiler.makeZSet(mapBody.getType()), source);
        this.addOperator(map);

        // Flatmap flattens the array
        DBSPVariablePath data = new DBSPVariablePath(mapBody.getType().ref());
        // This is not the timestamp type, since e can never be null.
        DBSPVariablePath e = new DBSPVariablePath(hopType);
        DBSPExpression collectionExpression = data.deref().field(nextIndex).borrow();

        List<DBSPStatement> statements = new ArrayList<>();
        String varName = "x";
        for (int i = 0; i < inputRowType.size(); i++) {
            DBSPLetStatement stat = new DBSPLetStatement(
                    varName + i, data.deref().field(i).applyCloneIfNeeded());
            statements.add(stat);
        }
        DBSPLetStatement array = new DBSPLetStatement("array", collectionExpression);
        statements.add(array);
        DBSPLetStatement clone = new DBSPLetStatement(
                "array_clone", array.getVarReference().deref().applyClone());
        statements.add(clone);
        DBSPExpression iter = new DBSPApplyMethodExpression("into_iter",
                DBSPTypeAny.getDefault(), clone.getVarReference().applyClone());
        DBSPExpression[] resultFields = new DBSPExpression[type.size()];
        for (int i = 0; i < inputRowType.size(); i++)
            resultFields[i] = statements.get(i).to(DBSPLetStatement.class).getVarReference().applyClone();
        nextIndex = inputRowType.size();
        resultFields[nextIndex++] = e;
        resultFields[nextIndex] = ExpressionCompiler.makeBinaryExpression(node,
                // not the timestampType, but the hopType
                hopType, DBSPOpcode.ADD, e, size);

        DBSPExpression toTuple = new DBSPTupleExpression(resultFields).closure(e.asParameter());
        DBSPExpression makeTuple = new DBSPApplyMethodExpression(node,
                "map", DBSPTypeAny.getDefault(), iter, toTuple);
        DBSPBlockExpression block = new DBSPBlockExpression(statements, makeTuple);

        DBSPOperator result = new DBSPFlatMapOperator(node, block.closure(data.asParameter()),
                TypeCompiler.makeZSet(type), map);
        this.map(operator, result);
    }
}

package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.Expensive;
import org.dbsp.sqlCompiler.compiler.visitors.inner.ExpressionTranslator;
import org.dbsp.sqlCompiler.compiler.visitors.outer.temporal.ContainsNow;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.HashMap;
import java.util.Map;

/** Inline SQL UDF functions when
 * - they are simple
 * - have NOW() as an argument */
public class UDFInliner extends ExpressionTranslator {
    public final Map<String, DBSPFunction> udfMap = new HashMap<>();
    final ContainsNow cn;

    public UDFInliner(DBSPCompiler compiler) {
        super(compiler);
        this.cn = new ContainsNow(compiler, true);
    }

    @Override
    public void setCircuitContext(DBSPCircuit circuit) {
        for (var entry: circuit.declarationMap.entrySet()) {
            if (entry.getValue().item.is(DBSPFunctionItem.class)) {
                DBSPFunctionItem func = entry.getValue().item.to(DBSPFunctionItem.class);
                if (func.function.body != null) {
                    Utilities.putNew(this.udfMap, entry.getKey(), func.function);
                }
            }
        }
        super.setCircuitContext(circuit);
    }

    @Override
    public void postorder(DBSPApplyExpression expression) {
        DBSPExpression[] args = Linq.map(expression.arguments, this::getE, DBSPExpression.class);
        String function = expression.getFunctionName();
        boolean hasNow = false;
        for (var arg: args) {
            cn.apply(arg);
            if (cn.found()) {
                hasNow = true;
                break;
            }
        }
        if (function != null && this.udfMap.containsKey(function)) {
            DBSPClosureExpression func = this.udfMap.get(function).asClosure();
            boolean expensive = Expensive.isExpensive(this.compiler, func);
            if (!expensive || hasNow) {
                DBSPExpression inlined = func.call(args).reduce(this.compiler);
                this.map(expression, inlined);
                return;
            }
        }
        super.postorder(expression);
    }
}

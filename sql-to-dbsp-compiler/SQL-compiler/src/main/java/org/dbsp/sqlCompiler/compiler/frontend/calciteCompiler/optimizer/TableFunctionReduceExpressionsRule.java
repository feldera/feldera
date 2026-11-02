package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/** Reduces the constant expressions in the arguments of a table function call. */
public class TableFunctionReduceExpressionsRule
        extends RelRule<DefaultOptRuleConfig<TableFunctionReduceExpressionsRule>>
        implements TransformationRule {
    public TableFunctionReduceExpressionsRule() {
        super(CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalTableFunctionScan scan = call.rel(0);
        final List<RexNode> expList = Lists.newArrayList(scan.getCall());
        if (!ReduceExpressionsRule.reduceExpressions(
                scan, expList, RelOptPredicateList.EMPTY, false, true, false))
            return;
        call.transformTo(scan.copy(scan.getTraitSet(), scan.getInputs(), expList.get(0),
                scan.getElementType(), scan.getRowType(), scan.getColumnMappings()));
        call.getPlanner().prune(scan);
    }

    public static final DefaultOptRuleConfig<TableFunctionReduceExpressionsRule> CONFIG =
            DefaultOptRuleConfig.<TableFunctionReduceExpressionsRule>create()
                    .withOperandSupplier(
                            b -> b.operand(LogicalTableFunctionScan.class).anyInputs());
}

package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dbsp.util.Linq;

import java.util.Collections;
import java.util.List;

/** Extension to Calcite's {@link SqlToRelConverter}'s class.
 * We do exactly the same thing, but also attach ALIAS annotations to FROM nodes that have an AS alias,
 * so we can carry some alias information to the Rel representation. */
public class FelderaSqlToRelConverter extends SqlToRelConverter {
    public FelderaSqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,  @Nullable SqlValidator validator,
            Prepare.CatalogReader catalogReader, RelOptCluster cluster,
            SqlRexConvertletTable convertletTable, Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    /** Converts a balanced tree built by {@link BalanceSetOperations} to one set operation
     * with all the queries of the chain as inputs, the plan that UNION_MERGE builds from a short chain. */
    @Override
    protected RelNode convertSetOp(SqlCall call) {
        if (!(call.getOperator() instanceof BalanceSetOperations.BalancedSetOperator balanced))
            return super.convertSetOp(call);
        List<SqlNode> operands = BalanceSetOperations.chainOperands(call);
        List<RelNode> inputs = Linq.map(operands, o -> this.convertQueryRecursive(o, false, null).project());
        if (call.getKind() == SqlKind.UNION)
            return LogicalUnion.create(inputs, balanced.isAll());
        return LogicalIntersect.create(inputs, balanced.isAll());
    }

    @Override
    protected void convertFrom(Blackboard bb, @Nullable SqlNode from, @Nullable List<String> fieldNames) {
        super.convertFrom(bb, from, fieldNames);
        if (from != null && from.getKind() == SqlKind.AS) {
            SqlCall call = (SqlCall) from;
            RelNode root = bb.root;

            // Attach the original AS alias as a HINT_ALIAS to the rel node.
            if (root instanceof Hintable hintable) {
                RelHint alias = RelHint.builder(SqlToRelCompiler.HINT_ALIAS)
                        .hintOption(call.operand(1).toString())
                        .position(call.getParserPosition())
                        .build();
                RelNode newRoot = hintable.attachHints(Collections.singletonList(alias));
                boolean isLeaf = this.leaves.containsKey(root);
                if (isLeaf)
                    leaves.remove(root);
                bb.setRoot(newRoot, isLeaf);
            }
        }
    }
}

package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that replaces an IN sub-query over a constant array with an IN list:
 * {@code x IN (SELECT * FROM UNNEST(ARRAY[c1, ..., cn]))} becomes {@code x IN (c1, ..., cn)}.
 *
 * <pre>
 * IN($x, {
 *   LogicalProject(c=[$0])                         optional; the validator may cast $0
 *     Uncollect
 *       LogicalProject(EXPR$0=[ARRAY(c1, ..., cn)])
 *         LogicalValues(tuples=[[{ 0 }]])
 * })
 * </pre>
 * becomes {@code SEARCH($x, Sarg[c1, ..., cn])}, or {@code =($x, c1)} for one element.
 *
 * <p>Both have the same value in every context, including NOT IN: an IN list and an
 * IN sub-query over the same non-empty set of values follow the same three-valued logic.
 * The rule only handles arrays of non-null literals whose values have the type of x,
 * possibly after the casts that the validator adds to the sub-query, or are strings that
 * fit in a VARCHAR x. */
public class InUnnestToInListRule
        extends RelRule<DefaultOptRuleConfig<InUnnestToInListRule>>
        implements TransformationRule {
    protected InUnnestToInListRule() {
        super(CONFIG);
    }

    /** The values of the column that 'rel' produces from a constant array of non-null literals:
     * the literals, with the casts that 'rel' applies to the column; null if 'rel' has another shape */
    static @Nullable List<RexNode> unnestedValues(RexBuilder builder, RelNode rel) {
        RelNode node = rel.stripped();
        // Innermost cast first
        List<RelDataType> castTypes = new ArrayList<>();
        if (node instanceof Project project) {
            // The SELECT list of the sub-query: its single column, which the validator may cast
            // more than once
            List<RexNode> projects = project.getProjects();
            if (projects.size() != 1)
                return null;
            RexNode column = projects.get(0);
            while (column.getKind() == SqlKind.CAST) {
                castTypes.add(0, column.getType());
                column = ((RexCall) column).getOperands().get(0);
            }
            if (!(column instanceof RexInputRef ref) || ref.getIndex() != 0)
                return null;
            node = project.getInput().stripped();
        }
        if (!(node instanceof Uncollect uncollect) || uncollect.withOrdinality
                || uncollect.getRowType().getFieldCount() != 1)
            return null;
        if (!(uncollect.getInput().stripped() instanceof Project arrayProject)
                || arrayProject.getProjects().size() != 1)
            return null;
        if (!(arrayProject.getInput().stripped() instanceof Values values) || values.getTuples().size() != 1)
            return null;
        RexNode array = arrayProject.getProjects().get(0);
        if (array.getKind() != SqlKind.ARRAY_VALUE_CONSTRUCTOR)
            return null;
        List<RexNode> result = new ArrayList<>();
        for (RexNode element : ((RexCall) array).getOperands()) {
            if (!(element instanceof RexLiteral literal) || literal.isNull())
                return null;
            RexNode value = literal;
            for (RelDataType castType : castTypes)
                value = builder.makeCast(castType, value);
            result.add(value);
        }
        return result.isEmpty() ? null : result;
    }

    /** Replaces the IN sub-queries over constant arrays with IN lists */
    static class Rewriter extends RexShuttle {
        final RexBuilder builder;

        Rewriter(RexBuilder builder) {
            this.builder = builder;
        }

        /** 'value' as a value of 'type': unchanged if it has that type, cast if it is a string
         * literal that fits in a VARCHAR 'type', null otherwise.  The join that the sub-query
         * becomes compares both sides in their common type, which is 'type' in these cases. */
        @Nullable RexNode castValue(RexNode value, RelDataType type) {
            RelDataTypeFactory factory = this.builder.getTypeFactory();
            RelDataType valueType = value.getType();
            if (SqlTypeUtil.equalSansNullability(factory, valueType, type))
                return value;
            boolean fitsVarchar = value instanceof RexLiteral
                    && SqlTypeUtil.isCharacter(valueType)
                    && type.getSqlTypeName() == SqlTypeName.VARCHAR
                    && (type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                        || valueType.getPrecision() <= type.getPrecision());
            if (!fitsVarchar)
                return null;
            return this.builder.makeCast(factory.createTypeWithNullability(type, false), value);
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (subQuery.getKind() != SqlKind.IN || subQuery.getOperands().size() != 1)
                return super.visitSubQuery(subQuery);
            List<RexNode> values = unnestedValues(this.builder, subQuery.rel);
            if (values == null)
                return super.visitSubQuery(subQuery);
            RexNode operand = subQuery.getOperands().get(0).accept(this);
            List<RexNode> converted = new ArrayList<>(values.size());
            for (RexNode value : values) {
                RexNode cast = this.castValue(value, operand.getType());
                if (cast == null)
                    return super.visitSubQuery(subQuery);
                converted.add(cast);
            }
            RexNode inList = this.builder.makeIn(operand, converted);
            // A different type would change the row type of a Project
            if (!inList.getType().equals(subQuery.getType()))
                return super.visitSubQuery(subQuery);
            return inList;
        }
    }

    static boolean hasSubQuery(RelNode rel) {
        if (rel instanceof Filter filter)
            return RexUtil.SubQueryFinder.containsSubQuery(filter);
        if (rel instanceof Project project)
            return RexUtil.SubQueryFinder.containsSubQuery(project);
        if (rel instanceof Join join)
            return RexUtil.SubQueryFinder.containsSubQuery(join);
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        RelNode rewritten = rel.accept(new Rewriter(rel.getCluster().getRexBuilder()));
        if (rewritten != rel)
            call.transformTo(rewritten);
    }

    public static final DefaultOptRuleConfig<InUnnestToInListRule> CONFIG =
            DefaultOptRuleConfig.<InUnnestToInListRule>create()
                    .withOperandSupplier(b0 -> b0.operand(RelNode.class)
                            .predicate(InUnnestToInListRule::hasSubQuery)
                            .anyInputs());
}

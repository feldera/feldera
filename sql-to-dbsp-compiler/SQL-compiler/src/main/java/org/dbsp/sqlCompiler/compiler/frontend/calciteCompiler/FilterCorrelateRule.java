/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * A modified version of {@link org.apache.calcite.rel.rules.FilterCorrelateRule}
 * which does not push predicates that contain other correlated variables.
 *
 * <p>Planner rule that pushes a {@link Filter} above a {@link Correlate} into the
 * inputs of the Correlate.
 *
 * @see CoreRules#FILTER_CORRELATE
 */
public class FilterCorrelateRule
        extends RelRule<org.apache.calcite.rel.rules.FilterCorrelateRule.Config>
        implements TransformationRule {

    /** Creates a FilterCorrelateRule. */
    protected FilterCorrelateRule(org.apache.calcite.rel.rules.FilterCorrelateRule.Config config) {
        super(config);
    }

    //~ Methods ----------------------------------------------------------------

    static class CorrelVariableFinder extends RexShuttle {
        boolean found = false;

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
            found = true;
            return correlVariable;
        }

        static boolean has(RexNode root) {
            CorrelVariableFinder finder = new CorrelVariableFinder();
            root.accept(finder);
            return finder.found;
        }
    }


    @Override public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final Correlate corr = call.rel(1);

        List<RexNode> aboveFilters =
                RelOptUtil.conjunctions(filter.getCondition());

        List<RexNode> leftFilters = new ArrayList<>();
        List<RexNode> rightFilters = new ArrayList<>();

	    List<RexNode> ineligible = new ArrayList<>();
	    List<RexNode> eligible = new ArrayList<>();
        for (RexNode f: aboveFilters) {
            if (CorrelVariableFinder.has(f)) {
                ineligible.add(f);
            } else {
                eligible.add(f);
            }
        }

        aboveFilters = eligible;
        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        RelOptUtil.classifyFilters(
                corr,
                aboveFilters,
                false,
                true,
                corr.getJoinType().canPushRightFromAbove(),
                aboveFilters,
                leftFilters,
                rightFilters);

        if (leftFilters.isEmpty() && rightFilters.isEmpty()) {
            // no filters got pushed
            return;
        }

	    aboveFilters.addAll(ineligible);

        // Create Filters on top of the children if any filters were
        // pushed to them.
        final RexBuilder rexBuilder = corr.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();
        final RelNode leftRel =
                relBuilder.push(corr.getLeft()).filter(leftFilters).build();
        final RelNode rightRel =
                relBuilder.push(corr.getRight()).filter(rightFilters).build();

        // Create the new Correlate
        RelNode newCorrRel =
                corr.copy(corr.getTraitSet(), ImmutableList.of(leftRel, rightRel));

        call.getPlanner().onCopy(corr, newCorrRel);

        if (!leftFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, leftRel);
        }
        if (!rightFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, rightRel);
        }

        // Create a Filter on top of the join if needed
        relBuilder.push(newCorrRel);
        relBuilder.filter(
                RexUtil.fixUp(rexBuilder, aboveFilters,
                        RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));

        call.transformTo(relBuilder.build());
    }

    public static final FilterCorrelateRule FILTER_CORRELATE =
            new FilterCorrelateRule(org.apache.calcite.rel.rules.FilterCorrelateRule.Config.DEFAULT);
}

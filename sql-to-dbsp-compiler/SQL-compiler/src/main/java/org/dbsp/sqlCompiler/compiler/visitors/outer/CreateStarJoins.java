package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPDifferentiateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPMapIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinIndexOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPVariablePath;
import org.dbsp.sqlCompiler.ir.expression.NoExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;
import org.dbsp.util.Maybe;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Recognize patterns of trees of joins that can be converted into a single @{link DBSPStarJoinOperator}
 * (or {@link DBSPStarJoinIndexOperator}.
 * This pass runs before incrementalization, so it looks for {@link DBSPStreamJoinOperator} and other
 * non-incremental operators.
 *
 * <p>What makes this pass more complicated is the reverse engineering of the keys.  In a star
 * join all keys are obtained from the input collections, but in a tree of joins the keys for
 * the lower joins are produced from fields of the inner collections.  These keys must be translated
 * into accesses to the input collections of the tree -- if possible.  If not possible, this tree of
 * joins cannot be implemented as a {@link DBSPStarJoinOperator}. */
public class CreateStarJoins extends Passes {
    public CreateStarJoins(DBSPCompiler compiler) {
        super("CreateStarJoins", compiler);
        Graph g = new Graph(compiler);
        this.add(g);
        FindJoinTrees fjt = new FindJoinTrees(compiler, g.graphs);
        this.add(fjt);
        this.add(new ReplaceStarJoins(compiler, fjt.joinTrees));
    }

    /**
     * Represents a JoinTree that can be synthesized as a StarJoin.
     * The requirement is for all keys to have the same type.
     *
     * @param root     Join operator at the root.
     * @param realRoot Real operator in tree which is the root; can be {@link DBSPMapIndexOperator} after a join.
     * @param closure  Closure that can be used for the StarJoin equivalent to this tree.
     * @param inputs   Inputs to the star join operator.
     * @param left     Left subtree, if any.
     * @param right    Right subtree, if any.
     */
    record JoinTree(DBSPJoinBaseOperator root, DBSPSimpleOperator realRoot,
                    DBSPClosureExpression closure, List<OutputPort> inputs,
                    @Nullable JoinTree left, @Nullable JoinTree right) {
        JoinTree {
            Utilities.enforce(closure.parameters.length == inputs.size() + 1);
        }

        @Override
        public String toString() {
            return "realRoot=" + this.realRoot + ", inputs=" + this.inputs;
        }

        int size() {
            return this.inputs.size() - 1;
        }

        /** Create a new JoinTree
         *
         * @param root       Root join for this tree
         * @param realRoot   Real operator at the root; can be a {@link DBSPMapIndexOperator} after a join.
         * @param leftChild  Left child tree; may be null.
         * @param rightChild Right child tree.
         * @return           The new JoinTree created, with the closure synthesized for a star join.
         */
        static JoinTree create(
                DBSPCompiler compiler,
                DBSPJoinBaseOperator root,
                DBSPSimpleOperator realRoot,
                @Nullable JoinTree leftChild,
                @Nullable JoinTree rightChild) {
            final List<OutputPort> newInputs = new ArrayList<>();
            final DBSPExpression left, right;
            DBSPVariablePath key = root.getKeyType().ref().var();
            List<DBSPVariablePath> vars = new ArrayList<>();
            vars.add(key);

            if (leftChild != null) {
                newInputs.addAll(leftChild.inputs);
                List<DBSPVariablePath> leftVars = Linq.map(leftChild.inputs,
                        i -> i.getOutputIndexedZSetType().elementType.ref().var());
                vars.addAll(leftVars);

                leftVars.add(0, key);
                left = leftChild.closure()
                        .call(Linq.map(leftVars, v -> v.to(DBSPExpression.class)))
                        .field(1)
                        .borrow()
                        .reduce(compiler);
            } else {
                newInputs.add(root.left());
                DBSPVariablePath var = root.left().getOutputIndexedZSetType().elementType.ref().var();
                vars.add(var);
                left = var;
            }
            if (rightChild != null) {
                newInputs.addAll(rightChild.inputs);
                List<DBSPVariablePath> rightVars = Linq.map(rightChild.inputs,
                        i -> i.getOutputIndexedZSetType().elementType.ref().var());
                vars.addAll(rightVars);

                rightVars.add(0, key);
                right = rightChild.closure()
                        .call(Linq.map(rightVars, v -> v.to(DBSPExpression.class)))
                        .field(1)
                        .borrow()
                        .reduce(compiler);
            } else {
                newInputs.add(root.right());
                DBSPVariablePath var = root.right().getOutputIndexedZSetType().elementType.ref().var();
                vars.add(var);
                right = var;
            }

            DBSPVariablePath[] params = vars.toArray(DBSPVariablePath[]::new);
            DBSPClosureExpression closure = root.getClosureFunction().call(key, left, right).closure(params);
            return new JoinTree(root, realRoot, closure, newInputs, leftChild, rightChild);
        }

        DBSPType getKeyType() {
            return this.root.getKeyType();
        }
    }

    static class JoinTrees {
        /** For each operator that corresponds to a join tree, the largest join tree rooted there.
         * The key can be a {@link DBSPMapIndexOperator}, a {@link DBSPStreamJoinOperator} or
         * a {@link DBSPStreamJoinIndexOperator}. */
        final Map<DBSPSimpleOperator, JoinTree> treeByRoot;
        /** The set of join operators which have a join tree but are not a child of another join tree */
        final Set<DBSPJoinBaseOperator> maximalJoinRoots;

        JoinTrees() {
            this.treeByRoot = new HashMap<>();
            this.maximalJoinRoots = new HashSet<>();
        }

        boolean isMaximal(DBSPJoinBaseOperator join) {
            return this.maximalJoinRoots.contains(join);
        }

        @Nullable JoinTree get(DBSPSimpleOperator root) {
            return this.treeByRoot.get(root);
        }
    }

    static class FindJoinTrees extends CircuitWithGraphsVisitor {
        JoinTrees joinTrees;

        public FindJoinTrees(DBSPCompiler compiler, CircuitGraphs graphs) {
            super(compiler, graphs);
            this.joinTrees = new JoinTrees();
        }

        @Override
        public VisitDecision preorder(DBSPNestedOperator operator) {
            // Do not look inside nested operators
            return VisitDecision.STOP;
        }

        @Nullable
        JoinTree checkInput(DBSPJoinBaseOperator join, int input) {
            OutputPort in = join.inputs.get(input);
            CircuitGraph graph = this.getGraph();
            if (graph.getFanout(in.node()) != 1 || !in.isSimpleNode())
                return null;

            JoinTree tree = this.joinTrees.get(in.simpleNode());
            if (tree == null) {
                return null;
            }

            Utilities.enforce(tree.root.is(DBSPStreamJoinIndexOperator.class));
            if (!tree.getKeyType().sameType(join.getKeyType()))
                return null;

            // Check if the key produced by the joinIndex operator is the same as the input key of the join
            DBSPClosureExpression closure = tree.root.getClosureFunction();
            DBSPVariablePath var = closure.getResultType().ref().var();
            DBSPClosureExpression project = var.deref().field(0).closure(var);
            DBSPClosureExpression keyPart = project.applyAfter(this.compiler, closure, Maybe.YES);
            // Substitute data fields with some "constants"
            DBSPVariablePath keyVar = keyPart.parameters[0].asVariable();
            DBSPExpression leftData = new NoExpression(keyPart.parameters[1].getType());
            DBSPExpression rightData = new NoExpression(keyPart.parameters[2].getType());
            DBSPClosureExpression keyToKeyFunction = keyPart
                    .call(keyVar, leftData, rightData)
                    .closure(keyVar)
                    .reduce(this.compiler)
                    .to(DBSPClosureExpression.class);
            boolean isIdentity = RemoveIdentityOperators.isIdentityFunction(keyToKeyFunction);
            if (!isIdentity)
                return null;
            return tree;
        }

        void processStreamJoin(DBSPJoinBaseOperator operator) {
            JoinTree left = this.checkInput(operator, 0);
            JoinTree right = this.checkInput(operator, 1);
            JoinTree tree = JoinTree.create(this.compiler, operator, operator, left, right);
            if (left != null)
                // We have found a bigger tree
                this.joinTrees.maximalJoinRoots.remove(left.root);
            if (right != null)
                // We have found a bigger tree
                this.joinTrees.maximalJoinRoots.remove(right.root);
            this.joinTrees.maximalJoinRoots.add(operator);
            Utilities.putNew(this.joinTrees.treeByRoot, operator, tree);
        }

        @Override
        public void postorder(DBSPStreamJoinIndexOperator operator) {
            this.processStreamJoin(operator);
        }

        @Override
        public void postorder(DBSPStreamJoinOperator operator) {
            this.processStreamJoin(operator);
        }

        @Override
        public void postorder(DBSPMapIndexOperator operator) {
            // If the input is a Join which has an attached JoinTree, create a JoinTree for this operator
            // as if this was a DBSPStreamJoinIndex operator instead of a MapIndex following a StreamJoin.
            if (operator.input().node().is(DBSPStreamJoinOperator.class)) {
                DBSPStreamJoinOperator join = operator.input().node().to(DBSPStreamJoinOperator.class);
                CircuitGraph graph = this.getGraph();
                if (graph.getFanout(join) == 1) {
                    JoinTree tree = this.joinTrees.get(join);
                    if (tree != null) {
                        var joinIndex = OptimizeProjectionVisitor.mapIndexAfterJoin(this.compiler, join, operator)
                                .to(DBSPStreamJoinIndexOperator.class);
                        JoinTree result = JoinTree.create(this.compiler, joinIndex, operator, tree.left, tree.right);
                        Utilities.putNew(this.joinTrees.treeByRoot, operator, result);
                    }
                }
            }
            super.postorder(operator);
        }
    }

    static class ReplaceStarJoins extends CircuitCloneVisitor {
        final JoinTrees joinTrees;

        public ReplaceStarJoins(DBSPCompiler compiler, JoinTrees joinTrees) {
            super(compiler, false);
            this.joinTrees = joinTrees;
        }

        @Override
        public void postorder(DBSPStreamJoinOperator operator) {
            if (this.joinTrees.isMaximal(operator)) {
                JoinTree tree = this.joinTrees.get(operator);
                Utilities.enforce(tree != null);
                if (tree.size() > 1) {
                    List<OutputPort> newInputs = Linq.map(tree.inputs, this::mapped);
                    List<OutputPort> diffs = new ArrayList<>();
                    for (var port: newInputs) {
                        var diff = new DBSPDifferentiateOperator(operator.getRelNode(), port);
                        this.addOperator(diff);
                        diffs.add(diff.outputPort());
                    }
                    DBSPStarJoinOperator star = new DBSPStarJoinOperator(
                            operator.getRelNode(), operator.outputType, tree.closure, operator.isMultiset, diffs);
                    this.addOperator(star);
                    DBSPIntegrateOperator integrate = new DBSPIntegrateOperator(operator.getRelNode(), star.outputPort());
                    this.map(operator, integrate);
                    return;
                }
            }
            super.postorder(operator);
        }

        @Override
        public void postorder(DBSPStreamJoinIndexOperator operator) {
            if (this.joinTrees.isMaximal(operator)) {
                JoinTree tree = this.joinTrees.get(operator);
                Utilities.enforce(tree != null);
                if (tree.size() > 1) {
                    List<OutputPort> newInputs = Linq.map(tree.inputs, this::mapped);
                    DBSPStarJoinIndexOperator star = new DBSPStarJoinIndexOperator(
                            operator.getRelNode(), operator.outputType, tree.closure, operator.isMultiset, newInputs);
                    this.map(operator, star);
                    return;
                }
            }
            super.postorder(operator);
        }
    }
}

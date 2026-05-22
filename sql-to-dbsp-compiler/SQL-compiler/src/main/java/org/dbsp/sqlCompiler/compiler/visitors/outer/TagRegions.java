package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.annotation.Annotation;
import org.dbsp.sqlCompiler.circuit.annotation.Annotations;
import org.dbsp.sqlCompiler.circuit.annotation.GlobalAggregate;
import org.dbsp.sqlCompiler.circuit.annotation.RegionAnnotation;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.util.Linq;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Find operators that cannot be in the same DBSP circuit region and even if they have a {@link RegionAnnotation}
 * annotation. Regions have to be connected components that do not create cycles when collapsed to a single node,
 * so we check this property.  If the nodes with some annotation do not form a connected component,
 * we do not group them into a region, by essentially removing this annotation from the nodes. */
public class TagRegions extends Passes {
    public TagRegions(DBSPCompiler compiler) {
        super("TagRegions", compiler);
        Set<Annotation> remove = new HashSet<>();

        this.add(new DetectConflicts(compiler, remove));
        this.add(new RemoveRegions(compiler, remove));
    }

    /** Detect region annotations that have to be removed because they are not
     * present on a connected component of the circuit.
     *
     * <p>The algorithm tracks for each operator the annotations seen it all its
     * predecessors (up to circuit inputs).  If an input does NOT have an
     * annotation, yet one of its predecessors does, the annotation is removed.
      */
    static class DetectConflicts extends CircuitVisitor {
        final Set<Annotation> toRemove;
        final Map<DBSPOperator, Set<Annotation>> allPredecessors;

        public DetectConflicts(DBSPCompiler compiler, Set<Annotation> toRemove) {
            super(compiler);
            this.toRemove = toRemove;
            this.allPredecessors = new HashMap<>();
        }

        public void postorder(DBSPOperator operator) {
            List<RegionAnnotation> regions = operator.annotations.get(RegionAnnotation.class);
            // Remove the regions that have to be removed anyway
            regions = Linq.where(regions, x -> !this.toRemove.contains(x));
            final RegionAnnotation annotation;
            if (regions.size() > 1) {
                // same operator in multiple regions;
                // remove all except the first alphabetically
                regions.sort(Comparator.comparing(RegionAnnotation::asVarName));
                for (int i = 1; i < regions.size(); i++) {
                    this.toRemove.add(regions.get(i));
                }
                annotation = regions.get(0);
            } else if (regions.size() == 1) {
                annotation = regions.get(0);
            } else {
                annotation = null;
            }

            Set<Annotation> ancestors = new HashSet<>();
            for (OutputPort input: operator.inputs) {
                DBSPOperator in = input.operator;
                if (annotation != null) {
                    if (in.annotations.contains(a -> a.equals(annotation)))
                        // input has it as well, we're good
                        continue;
                    // input does not have it
                    if (this.allPredecessors.get(in).contains(annotation))
                        // But one of its input does; remove it
                        this.toRemove.add(annotation);
                }
                ancestors.addAll(in.annotations.annotations);
            }
            ancestors.addAll(regions);
            Utilities.putNew(this.allPredecessors, operator, ancestors);
        }
    }

    /** Remove all annotations that appear in the toRemove set.
     * Note: this seems to be a read-only visitor, but it does mutate the annotations. */
    static class RemoveRegions extends CircuitVisitor {
        final Set<Annotation> toRemove;

        public RemoveRegions(DBSPCompiler compiler, Set<Annotation> toRemove) {
            super(compiler);
            this.toRemove = toRemove;
        }

        public void postorder(DBSPOperator operator) {
            List<Annotation> afterRemove = Linq.where(
                    operator.annotations.annotations, x -> !this.toRemove.contains(x));
            if (afterRemove.size() != operator.annotations.size()) {
                Logger.INSTANCE.belowLevel(this, 1)
                        .append("Removed ")
                        .append(operator.annotations.size() - afterRemove.size())
                        .append(" annotations from ")
                        .appendSupplier(operator::toString);
                operator.annotations.replace(new Annotations(afterRemove));
            }
        }
    }
}

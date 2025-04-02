package org.dbsp.sqlCompiler.compiler;

import org.dbsp.util.IHasId;

import java.util.HashSet;
import java.util.Set;

/** A class that tracks objects that have been already analyzed */
public class AnalyzedSet<T extends IHasId> {
    final Set<Long> operationsAnalyzed;

    public AnalyzedSet() {
        this.operationsAnalyzed = new HashSet<>();
    }

    public boolean contains(T object) {
        return this.operationsAnalyzed.contains(object.getId());
    }

    /** Mark the object as analyzed and return true if it was already analyzed */
    public boolean done(T object) {
        long id = object.getId();
        boolean done = this.operationsAnalyzed.contains(id);
        this.operationsAnalyzed.add(id);
        return done;
    }
}

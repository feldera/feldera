package org.dbsp.sqlCompiler.compiler.backend.rust.multi;

import java.util.HashSet;
import java.util.Set;

/** Information that may be global across multiple circuits that generate code in the same project. */
public class ProjectDeclarations {
    /** Declarations made outside circuits, which may be shared between many circuits. */
    final Set<String> perFileDeclarations;

    public ProjectDeclarations() {
        this.perFileDeclarations = new HashSet<>();
    }

    public boolean contains(String declaration) {
        return this.perFileDeclarations.contains(declaration);
    }

    public void declare(String declaration) {
        this.perFileDeclarations.add(declaration);
    }
}

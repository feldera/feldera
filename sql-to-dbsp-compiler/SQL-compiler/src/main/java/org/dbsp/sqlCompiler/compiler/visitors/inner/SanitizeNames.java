package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.util.FreshName;

import java.util.HashSet;
import java.util.Set;

public class SanitizeNames extends Passes {
    public SanitizeNames(IErrorReporter reporter) {
        super(reporter);
        Set<String> identifiers = new HashSet<>();
        super.add(new CollectIdentifiers(reporter, identifiers).getCircuitVisitor());
        super.add(new SanitizeStructNames(reporter, new FreshName(identifiers), true));
    }
}

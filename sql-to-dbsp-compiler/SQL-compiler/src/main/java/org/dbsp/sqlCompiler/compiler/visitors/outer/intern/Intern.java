package org.dbsp.sqlCompiler.compiler.visitors.outer.intern;

import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceTableOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.DeadCode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Performs interning for some scalar fields driven by user annotations */
public class Intern extends Passes {
    public static class InternedColumnList {
        final Set<Integer> columns;

        InternedColumnList() {
            this.columns = new HashSet<>();
        }

        public void add(int column) {
            this.columns.add(column);
        }

        public int size() {
            return this.columns.size();
        }

        public boolean isEmpty() {
            return this.columns.isEmpty();
        }

        public boolean contains(int i) {
            return this.columns.contains(i);
        }

        @Override
        public String toString() {
            return this.columns.toString();
        }
    };

    final Map<DBSPSourceTableOperator, InternedColumnList> internedInputs;

    public Intern(DBSPCompiler compiler) {
        super("Intern", compiler);
        this.internedInputs = new HashMap<>();
        this.add(new FindInternedInputs(compiler, this.internedInputs));
        this.add(new RewriteInternedFields(compiler, this.internedInputs));
        this.add(new DeadCode(compiler, true, false));
    }
}

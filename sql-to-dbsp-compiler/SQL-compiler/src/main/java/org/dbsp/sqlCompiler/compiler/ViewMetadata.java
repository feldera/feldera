package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.util.Linq;

import java.util.List;

public class ViewMetadata {
    public final ProgramIdentifier viewName;
    public final List<ViewColumnMetadata> columns;
    public final SqlCreateView.ViewKind viewKind;
    // Only valid if positive
    public final int emitFinalColumn;
    public final boolean recursive;
    /** True if this is a system view */
    public final boolean system;

    public ViewMetadata(ProgramIdentifier viewName,
                        List<ViewColumnMetadata> columns, SqlCreateView.ViewKind viewKind,
                        int emitFinalColumn, boolean recursive, boolean system) {
        this.viewName = viewName;
        this.columns = columns;
        this.viewKind = viewKind;
        this.emitFinalColumn = emitFinalColumn;
        this.recursive = recursive;
        this.system = system;
    }

    public int size() {
        return this.columns.size();
    }

    /** True if any column has LATENESS information */
    public boolean hasLateness() {
        return Linq.any(this.columns, m -> m.lateness != null);
    }
}

package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.util.Linq;

import java.util.List;

public class ViewMetadata {
    public final List<ViewColumnMetadata> columns;
    public final SqlCreateView.ViewKind viewKind;

    public ViewMetadata(List<ViewColumnMetadata> columns, SqlCreateView.ViewKind viewKind) {
        this.columns = columns;
        this.viewKind = viewKind;
    }

    public int size() {
        return this.columns.size();
    }

    /** True if any column has LATENESS information */
    public boolean hasLateness() {
        return Linq.any(this.columns, m -> m.lateness != null);
    }
}

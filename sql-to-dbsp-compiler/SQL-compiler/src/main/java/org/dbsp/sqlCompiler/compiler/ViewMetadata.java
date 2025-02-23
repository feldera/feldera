package org.dbsp.sqlCompiler.compiler;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.backend.ToJsonInnerVisitor;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.parser.PropertyList;
import org.dbsp.sqlCompiler.compiler.frontend.parser.SqlCreateView;
import org.dbsp.util.IJson;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Properties;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

public class ViewMetadata implements IJson {
    public final ProgramIdentifier viewName;
    public final List<ViewColumnMetadata> columns;
    public final SqlCreateView.ViewKind viewKind;
    // Only valid if positive
    public final int emitFinalColumn;
    public final boolean recursive;
    /** True if this is a system view */
    public final boolean system;
    @Nullable public final Properties properties;

    public ViewMetadata(ProgramIdentifier viewName,
                        List<ViewColumnMetadata> columns, SqlCreateView.ViewKind viewKind,
                        int emitFinalColumn, boolean recursive, boolean system,
                        @Nullable Properties properties) {
        this.viewName = viewName;
        this.columns = columns;
        this.viewKind = viewKind;
        this.emitFinalColumn = emitFinalColumn;
        this.recursive = recursive;
        this.system = system;
        this.properties = properties;
    }

    public int size() {
        return this.columns.size();
    }

    /** True if any column has LATENESS information */
    public boolean hasLateness() {
        return Linq.any(this.columns, m -> m.lateness != null);
    }

    @Override
    public void asJson(ToJsonInnerVisitor visitor) {
        JsonStream stream = visitor.stream;
        stream.beginObject();
        stream.label("viewName");
        this.viewName.asJson(visitor);
        stream.label("viewKind").append(this.viewKind.name());
        stream.label("emitFinalColumn").append(this.emitFinalColumn);
        stream.label("columns");
        stream.beginArray();
        for (ViewColumnMetadata key: this.columns)
            key.asJson(visitor);
        stream.endArray();
        stream.label("recursive").append(this.recursive);
        stream.label("system").append(this.system);
        if (this.properties != null) {
            stream.label("properties");
            this.properties.asJson(visitor);
        }
        stream.endObject();
    }

    public static ViewMetadata fromJson(JsonNode node, JsonDecoder decoder) {
        ProgramIdentifier viewName = ProgramIdentifier.fromJson(Utilities.getProperty(node, "viewName"));
        int emitFinalColumn = Utilities.getIntProperty(node, "emitFinalColumn");
        boolean recursive = Utilities.getBooleanProperty(node, "recursive");
        boolean system = Utilities.getBooleanProperty(node, "system");
        SqlCreateView.ViewKind viewKind = SqlCreateView.ViewKind.valueOf(Utilities.getStringProperty(node, "viewKind"));
        List<ViewColumnMetadata> columns = Linq.list(
                Linq.map(Utilities.getProperty(node, "columns").elements(),
                        e -> ViewColumnMetadata.fromJson(e, decoder)));
        Properties properties = null;
        if (node.has("properties"))
             properties = Properties.fromJson(Utilities.getProperty(node, "properties"));
        return new ViewMetadata(viewName, columns, viewKind, emitFinalColumn, recursive, system, properties);
    }
}

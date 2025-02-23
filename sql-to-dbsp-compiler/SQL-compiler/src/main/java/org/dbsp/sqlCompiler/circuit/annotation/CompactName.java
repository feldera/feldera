package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Stores the output stream name used for an operator.
 * Used when emitting the Rust code to make operator names more readable. */
public class CompactName extends Annotation {
    public final String name;

    public CompactName(String name) {
        this.name = name;
    }

    @Override
    public boolean invisible() {
        return true;
    }

    @Nullable
    public static String getCompactName(DBSPOperator operator) {
        List<Annotation> name = operator.annotations.get(t -> t.is(CompactName.class));
        if (!name.isEmpty()) {
            // there should be only one
            return name.get(0).to(CompactName.class).name;
        }
        return null;
    }

    public static CompactName fromJson(JsonNode node) {
        String name = Utilities.getStringProperty(node, "name");
        return new CompactName(name);
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this);
        stream.label("name");
        stream.append(this.name);
        stream.endObject();
    }
}

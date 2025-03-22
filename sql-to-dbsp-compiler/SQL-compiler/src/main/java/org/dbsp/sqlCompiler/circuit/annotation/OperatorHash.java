package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.util.HashString;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Stores a hash value for each operator */
public class OperatorHash extends Annotation {
    public final HashString hash;
    public final boolean global;

    public OperatorHash(HashString hash, boolean global) {
        this.hash = hash;
        this.global = global;
    }

    @Override
    public boolean invisible() {
        return true;
    }

    public boolean isGlobal(boolean global) {
        return this.global == global;
    }

    public static OperatorHash fromJson(JsonNode node) {
        String hash = Utilities.getStringProperty(node, "hash");
        boolean global = Utilities.getBooleanProperty(node, "global");
        return new OperatorHash(new HashString(hash), global);
    }

    @Nullable
    public static HashString getHash(DBSPOperator operator, boolean global) {
        List<Annotation> name = operator.annotations.get(
                t -> t.is(OperatorHash.class) && t.to(OperatorHash.class).isGlobal(global));
        if (!name.isEmpty()) {
            // there should be only one
            return name.get(0).to(OperatorHash.class).hash;
        }
        return null;
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this);
        stream.label("hash");
        stream.append(this.hash.toString());
        stream.label("global");
        stream.append(this.global);
        stream.endObject();
    }

    @Override
    public String toString() {
        return (this.global ? "Global " : "") + "hash: " + this.hash;
    }
}

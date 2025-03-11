package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

/** Stores a hash value for each operator */
public class MerkleHash extends Annotation {
    public final String hash;

    public MerkleHash(String hash) {
        this.hash = hash;
    }

    @Override
    public boolean invisible() {
        return true;
    }

    public static MerkleHash fromJson(JsonNode node) {
        String hash = Utilities.getStringProperty(node, "hash");
        return new MerkleHash(hash);
    }

    @Nullable
    public static String getHash(DBSPOperator operator) {
        List<Annotation> name = operator.annotations.get(t -> t.is(MerkleHash.class));
        if (!name.isEmpty()) {
            // there should be only one
            return name.get(0).to(MerkleHash.class).hash;
        }
        return null;
    }

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this);
        stream.label("hash");
        stream.append(this.hash);
        stream.endObject();
    }
}

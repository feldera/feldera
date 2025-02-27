package org.dbsp.sqlCompiler.circuit.annotation;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Utilities;

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

    @Override
    public void asJson(JsonStream stream) {
        stream.beginObject().appendClass(this);
        stream.label("hash");
        stream.append(this.hash);
        stream.endObject();
    }
}

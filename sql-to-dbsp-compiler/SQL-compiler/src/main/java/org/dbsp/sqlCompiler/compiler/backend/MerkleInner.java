package org.dbsp.sqlCompiler.compiler.backend;

import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.HashString;
import org.dbsp.util.JsonStream;
import org.dbsp.util.Logger;

/* Converts each inner node into a hash of its Rust code. */
public class MerkleInner extends ToJsonInnerVisitor {
    // This has nothing to do with JSON, but it is invoked from
    // MerkleOuter, which extends ToJsonOuterVisitor, so this
    // has to extend ToJsonInnerVisitor.
    public MerkleInner(DBSPCompiler compiler, JsonStream stream) {
        super(compiler, stream, 0);
    }

    public static HashString hash(String data) {
        String result = DigestUtils.sha256Hex(data);
        Logger.INSTANCE.belowLevel(MerkleInner.class, 1)
                .append("Hashing '")
                .append(data)
                .append("' to ")
                .append(result)
                .newline();
        return new HashString(result);
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        String rust = ToRustInnerVisitor.toRustString(this.compiler, node, null, false);
        HashString hash = hash(rust);
        this.stream.append(hash.toString());
        return VisitDecision.STOP;
    }
}

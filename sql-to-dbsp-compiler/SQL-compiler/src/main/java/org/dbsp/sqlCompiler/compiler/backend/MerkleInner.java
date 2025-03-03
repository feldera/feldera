package org.dbsp.sqlCompiler.compiler.backend;

import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.JsonStream;

/* Converts each inner node into a hash of its Rust code. */
public class MerkleInner extends ToJsonInnerVisitor {
    // This has nothing to do with JSON, but it is invoked from
    // MerkleOuter, which extends ToJsonOuterVisitor, so this
    // has to also extend ToJsonInnerVisitor.
    public MerkleInner(DBSPCompiler compiler, JsonStream stream) {
        super(compiler, stream, 0);
    }

    public static String hash(String data) {
        return DigestUtils.sha256Hex(data);
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        String rust = node.toString();
        this.stream.append(this.hash(rust));
        // no traversal needed at all
        return VisitDecision.STOP;
    }
}

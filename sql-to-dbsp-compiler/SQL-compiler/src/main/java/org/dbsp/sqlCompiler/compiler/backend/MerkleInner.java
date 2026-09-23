package org.dbsp.sqlCompiler.compiler.backend;

import org.apache.commons.codec.digest.DigestUtils;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.ToRustInnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.aggregate.DBSPAggregateList;
import org.dbsp.sqlCompiler.ir.aggregate.IAggregate;
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

    /** Under {@code --gen2} an aggregate operator keeps its aggregate list, which has no Rust
     * form; hash the Rust code of its parts. */
    @Override
    public VisitDecision preorder(DBSPAggregateList list) {
        AggregateListParts parts = new AggregateListParts(this.compiler);
        list.accept(parts);
        this.stream.append(hash(parts.rust.toString()).toString());
        return VisitDecision.STOP;
    }

    /** Renders an aggregate list as its structure and the Rust code of every expression in it. */
    static class AggregateListParts extends InnerVisitor {
        final StringBuilder rust = new StringBuilder();

        AggregateListParts(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void property(String name) {
            this.rust.append(name).append(": ");
        }

        @Override
        public VisitDecision preorder(DBSPAggregateList list) {
            this.rust.append("AggregateList { ");
            return VisitDecision.CONTINUE;
        }

        @Override
        public void postorder(DBSPAggregateList list) {
            this.rust.append("}");
        }

        @Override
        public VisitDecision preorder(IAggregate aggregate) {
            this.rust.append(aggregate.getClass().getSimpleName()).append(" { ");
            return VisitDecision.CONTINUE;
        }

        @Override
        public void postorder(IAggregate aggregate) {
            this.rust.append("} ");
        }

        @Override
        public VisitDecision preorder(IDBSPInnerNode node) {
            this.rust.append(ToRustInnerVisitor.toRustString(this.compiler, node, null, false)).append("; ");
            return VisitDecision.STOP;
        }
    }

    @Override
    public VisitDecision preorder(IDBSPInnerNode node) {
        String rust = ToRustInnerVisitor.toRustString(this.compiler, node, null, false);
        HashString hash = hash(rust);
        this.stream.append(hash.toString());
        return VisitDecision.STOP;
    }
}

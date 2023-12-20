
package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/** A Visitor which computes a translation for each object.
 * @param <T> The type of objects produced by the translation. */
public class TranslateVisitor<T> extends InnerVisitor {
    static class TranslationMap<T> {
        /** Indexed by node id */
        final Map<Long, T> translation;
        // Only used for debugging
        final Map<Long, IDBSPInnerNode> node;

        TranslationMap() {
            this.translation = new HashMap<>();
            this.node = new HashMap<>();
        }

        public void putNew(IDBSPInnerNode node, T translation) {
            Utilities.putNew(this.translation, node.getId(), translation);
            Utilities.putNew(this.node, node.getId(), node);
        }

        public T get(IDBSPInnerNode node) {
            return Utilities.getExists(this.translation, node.getId());
        }

        @Nullable
        public T maybeGet(IDBSPInnerNode node) {
            return this.translation.get(node.getId());
        }

        public String toString() {
            IndentStream stream = new IndentStream(new StringBuilder());
            stream.append("[").increase();
            for (Map.Entry<Long, T> e: this.translation.entrySet()) {
                IDBSPInnerNode node = this.node.get(e.getKey());
                stream.append(node)
                        .append(" ")
                        .append(node.getId())
                        .append("=>")
                        .append(e.getValue().toString())
                        .newline();
            }
            return stream.decrease().append("]").toString();
        }
    }

    final TranslationMap<T> translationMap;

    public TranslateVisitor(IErrorReporter reporter) {
        super(reporter);
        this.translationMap = new TranslationMap<>();
    }

    public void set(IDBSPInnerNode node, T translation) {
       this.translationMap.putNew(node, translation);
    }

    public T get(IDBSPInnerNode node) {
        return this.translationMap.get(node);
    }

    @Nullable
    public T maybeGet(IDBSPInnerNode node) {
        return this.translationMap.maybeGet(node);
    }

    public void maybeSet(IDBSPInnerNode node, @Nullable T translation) {
        if (translation != null)
            this.translationMap.putNew(node, translation);
    }

    protected T analyze(IDBSPInnerNode node) {
        node.accept(this);
        return this.get(node);
    }

    @Nullable
    public T applyAnalysis(IDBSPInnerNode node) {
        this.apply(node);
        return this.maybeGet(node);
    }
}

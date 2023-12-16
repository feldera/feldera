package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.IDBSPDeclaration;
import org.dbsp.util.TriFunction;

import javax.annotation.Nullable;
import java.util.function.BiFunction;

/** Maps IDBSPDeclaration nodes to values of type V */
public class DeclarationValue<V> {
    /** Indexed by node id */
    final Substitution<Long, V> substitution;

    protected DeclarationValue(Substitution<Long, V> data) {
        this.substitution = data;
    }

    public DeclarationValue() {
        this.substitution = new Substitution<>();
    }

    public int size() {
        return this.substitution.size();
    }

    public boolean isEmpty() {
        return this.substitution.isEmpty();
    }

    public boolean containsKey(IDBSPDeclaration decl) {
        return this.substitution.containsKey(decl.getId());
    }

    @Nullable
    public V get(IDBSPDeclaration decl) {
        return this.substitution.get(decl.getId());
    }

    public void put(IDBSPDeclaration decl, V value) {
        this.substitution.put(decl.getId(), value);
    }

    public void substitute(IDBSPDeclaration decl, V value) {
        this.substitution.substitute(decl.getId(), value);
    }

    public void clear() {
        this.substitution.clear();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public DeclarationValue<V> clone() {
        return new DeclarationValue<>(this.substitution.clone());
    }

    public DeclarationValue<V> mergeWith(DeclarationValue<V> other,
                                         BiFunction<V, V, V> merger) {
        TriFunction<Long, V, V, V> substMerger = (k, v0, v1) -> merger.apply(v0, v1);
        Substitution<Long, V> subst = this.substitution.mergeWith(other.substitution, substMerger);
        return new DeclarationValue<>(subst);
    }

    @Override
    public String toString() {
        return this.substitution.toString();
    }
}

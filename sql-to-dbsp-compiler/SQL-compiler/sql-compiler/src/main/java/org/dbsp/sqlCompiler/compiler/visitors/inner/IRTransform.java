package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.util.ICastable;

import java.util.function.Function;

public interface IRTransform extends Function<IDBSPInnerNode, IDBSPInnerNode>, ICastable {}

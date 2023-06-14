package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;

import java.util.function.Function;

public interface IRTransform extends Function<IDBSPInnerNode, IDBSPInnerNode> {}

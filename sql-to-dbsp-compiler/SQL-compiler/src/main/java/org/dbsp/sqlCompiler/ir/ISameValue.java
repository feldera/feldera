package org.dbsp.sqlCompiler.ir;

/** Inferface implemented by expressions that may have constant value.
 * This is most useful for literals, but we don't have literals of type Tuple or Struct. */
public interface ISameValue extends IDBSPInnerNode {
    boolean sameValue(ISameValue expression);
}

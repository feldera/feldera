package org.dbsp.util;

/** Compare two objects of the same kind, return diff as a string */
public interface IDiff<T> {
    String diff(T other);
}

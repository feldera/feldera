package org.dbsp.simulator.util;

/**
 * Interface implemented by objects that can be as nicely indented strings.
 */
public interface ToIndentableString {
    IIndentStream toString(IIndentStream builder);
}

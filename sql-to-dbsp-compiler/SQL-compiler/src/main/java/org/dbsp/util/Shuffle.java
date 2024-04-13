package org.dbsp.util;

import java.util.List;

/** Interface describing an operator which shuffles the elements from a list.
 * The result may contain a subset of element from the original list, or duplicates. */
public interface Shuffle {
    <T> List<T> shuffle(List<T> data);
}

package org.dbsp.util;

import java.util.List;

/** Interface describing an operator which shuffles the elements from a list.
 * The result may contain a subset of element from the original list, or duplicates. */
public interface Shuffle {
    /** Length of input list that is shuffled */
    int inputLength();

    <T> List<T> shuffle(List<T> data);

    /** Compose two shuffles, applying this one after 'shuffle' */
    Shuffle after(Shuffle shuffle);

    /** True if the element with the specified index is emitted in the output */
    boolean emitsIndex(int index);

    /** This only works for shuffles which are permutations.
     * This returns the inverse permutation of this shuffle. */
    Shuffle invert();

    /** True if this shuffle is an identity permutation */
    boolean isIdentityPermutation();
}

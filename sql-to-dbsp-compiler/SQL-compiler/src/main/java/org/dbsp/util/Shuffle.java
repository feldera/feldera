package org.dbsp.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.errors.UnimplementedException;

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

    /** Append a description of the shuffle as a JSON object */
    void asJson(JsonStream stream);

    static Shuffle fromJson(JsonNode node) {
        String type = Utilities.getStringProperty(node, "class");
        if (type.equals("IdShuffle"))
            return IdShuffle.fromJson(node);
        else if (type.equals("ExplicitShuffle"))
            return ExplicitShuffle.fromJson(node);
        else
            throw new UnimplementedException();
    }
}

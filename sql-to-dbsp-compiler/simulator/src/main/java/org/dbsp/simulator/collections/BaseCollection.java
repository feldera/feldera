package org.dbsp.simulator.collections;

import org.dbsp.simulator.util.ICastable;
import org.dbsp.simulator.util.IndentStream;
import org.dbsp.simulator.util.ToIndentableString;

public abstract class BaseCollection<Weight> implements ICastable, ToIndentableString {
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        IndentStream stream = new IndentStream(builder);
        this.toString(stream);
        return stream.toString();
    }
}

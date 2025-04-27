package org.dbsp.util;

import java.util.ArrayList;
import java.util.List;

/** API for producing JSON documents */
public class JsonStream {
    static class Context implements ICastable {
        public int index;
    }

    static class InArray extends Context {}

    static class InObject extends Context {
        public boolean expectLabel = true;
    }

    final List<Context> context = new ArrayList<>();
    private final IIndentStream stream;

    public JsonStream(IIndentStream stream) {
        this.stream = stream;
    }

    public JsonStream append(String string) {
        this.value();
        try {
            string = Utilities.deterministicObjectMapper().writeValueAsString(string);
            this.stream.append(string);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public JsonStream appendNull() {
        this.value();
        this.stream.append("null");
        return this;
    }

    /** Append the class of the data to the stream */
    public <T> JsonStream appendClass(T data) {
        return this.label("class").append(data.getClass().getSimpleName());
    }

    void value() {
        if (this.context.isEmpty())
            return;
        Context last = Utilities.last(this.context);
        if (last.is(InArray.class)) {
            if (last.index != 0) {
                this.stream.append(",").newline();
            } else {
                this.stream.increase();
            }
            last.index++;
        } else {
            InObject io = last.to(InObject.class);
            if (io.expectLabel)
                throw new RuntimeException("Missing label");
            io.index++;
            io.expectLabel = true;
        }
    }

    public JsonStream append(boolean b) {
        this.value();
        this.stream.append(b);
        return this;
    }

    public JsonStream append(int v) {
        this.value();
        this.stream.append(v);
        return this;
    }

    public JsonStream append(long v) {
        this.value();
        this.stream.append(v);
        return this;
    }

    public JsonStream label(String label) {
        Utilities.enforce(!label.isEmpty());
        Context last = Utilities.last(this.context);
        InObject io = last.to(InObject.class,
                "Adding label but not within JsonObject");
        if (!io.expectLabel)
            throw new RuntimeException("Consecutive labels");
        io.expectLabel = false;
        if (io.index == 0)
            this.stream.increase();
        else
            this.stream.append(",");
        this.stream.appendJsonLabelAndColon(label);
        return this;
    }

    public void beginArray() {
        this.value();
        this.context.add(new InArray());
        this.stream.append("[");
    }

    public JsonStream endArray() {
        Context last = Utilities.removeLast(this.context);
        Utilities.enforce(last.is(InArray.class));
        if (last.index != 0)
            this.stream.newline().decrease();
        this.stream.append("]");
        return this;
    }

    public JsonStream beginObject() {
        this.value();
        this.context.add(new InObject());
        this.stream.append("{");
        return this;
    }

    public void endObject() {
        Context last = Utilities.removeLast(this.context);
        Utilities.enforce(last.is(InObject.class));
        if (last.index != 0)
            this.stream.newline().decrease();
        this.stream.append("}");
    }

    @Override
    public String toString() {
        return this.stream.toString();
    }
}

package org.dbsp.sqlCompiler.compiler.visitors;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitRewriter;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/** Collect running time for various visitors.
 * Note that running times are not cummulative - some visitors can invoke other visitors. */
public class VisitorProfiles {
    record Profile(long time, int invocations) {
        Profile add(long time) {
            return new Profile(this.time + time, this.invocations + 1);
        }
    }

    final Map<String, Profile> profiles;
    final Map<String, Long> running;

    public VisitorProfiles() {
        this.profiles = new HashMap<>();
        this.running = new HashMap<>();
    }

    public void clear() {
        this.profiles.clear();
        this.running.clear();
    }

    static String getName(InnerVisitor visitor) {
        return visitor.getClass().getSimpleName();
    }

    static String getName(CircuitVisitor visitor) {
        if (visitor.is(CircuitRewriter.class)) {
            String name = visitor.to(CircuitRewriter.class).transform.getClass().getSimpleName();
            return "Rewriter+" + name;
        }
        return visitor.getClass().getSimpleName();
    }

    void start(String visitor) {
        Long now = System.currentTimeMillis();
        this.running.put(visitor, now);
    }

    void stop(String visitor) {
        long end = System.currentTimeMillis();
        Long started = Utilities.getExists(this.running, visitor);
        Utilities.removeExists(this.running, visitor);
        Profile previous = this.profiles.getOrDefault(visitor, new Profile(0, 0));
        this.profiles.put(visitor, previous.add(end - started));
    }

    public void start(InnerVisitor visitor) {
        start(getName(visitor));
    }

    public void stop(InnerVisitor visitor) {
        stop(getName(visitor));
    }

    public void start(CircuitVisitor visitor) {
        start(getName(visitor));
    }

    public void stop(CircuitVisitor visitor) {
        stop(getName(visitor));
    }

    public String toString(String title, int cutoff) {
        StringBuilder builder = new StringBuilder();
        builder.append(title).append(System.lineSeparator());

        var list = Linq.where(Linq.list(this.profiles.entrySet()),
                e -> e.getValue().time > cutoff);
        list.sort(Comparator.comparingLong(a -> a.getValue().time));
        Collections.reverse(list);
        int len = 0;
        int valLen = 0;
        int countLen = 0;

        for (var e : list) {
            len = Math.max(len, e.getKey().length());
            String formattedNumber = DBSPCompiler.COMMA_FORMATTER.format(e.getValue().time);
            valLen = Math.max(valLen, formattedNumber.length());
            String formattedCount = DBSPCompiler.COMMA_FORMATTER.format(e.getValue().invocations);
            countLen = Math.max(countLen, formattedCount.length());
        }
        for (var e : list) {
            String formattedNumber = DBSPCompiler.COMMA_FORMATTER.format(e.getValue().time);
            formattedNumber = String.format("%" + valLen + "s", formattedNumber);
            String formattedCount = DBSPCompiler.COMMA_FORMATTER.format(e.getValue().invocations);
            formattedCount = String.format("%" + countLen + "s", formattedCount);
            builder.append(String.format("%" + len + "s", e.getKey()))
                    .append(" ")
                    .append(formattedNumber)
                    .append(" ")
                    .append(formattedCount)
                    .append(System.lineSeparator());
        }
        return builder.toString();
    }
}

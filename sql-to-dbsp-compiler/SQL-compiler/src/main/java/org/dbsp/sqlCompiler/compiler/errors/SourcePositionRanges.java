package org.dbsp.sqlCompiler.compiler.errors;

import org.dbsp.util.Linq;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** A set of source positions */
public class SourcePositionRanges implements Iterable<SourcePositionRange> {
    final List<SourcePositionRange> positions;

    public SourcePositionRanges(Iterable<SourcePositionRange> positions) {
        List<SourcePositionRange> pos = Linq.list(positions);
        this.positions = canonicalize(pos);
    }

    static private List<SourcePositionRange> canonicalize(List<SourcePositionRange> pos) {
        pos.sort(SourcePositionRanges::compare);
        List<SourcePositionRange> result = new ArrayList<>();
        SourcePositionRange prev = null;
        for (SourcePositionRange p: pos) {
            if (!p.isValid())
                continue;
            if (prev == null) {
                prev = p;
                continue;
            }
            if (prev.includes(p)) {
                continue;
            }
            if (prev.includes(p.start)) {
                prev = prev.merge(p);
                continue;
            }
            if (prev.adjacent(p)) {
                prev = prev.merge(p);
                continue;
            }
            result.add(prev);
            prev = p;
        }
        if (prev != null)
            result.add(prev);
        return result;
    }

    private static int compare(SourcePositionRange p0, SourcePositionRange p1) {
        return p0.start.compareTo(p1.start);
    }

    @Override
    public Iterator<SourcePositionRange> iterator() {
        return this.positions.iterator();
    }
}

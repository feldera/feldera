package org.dbsp.sqlCompiler.compiler.sql.tools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Bounded dependency ordering shared by expression and producer arenas. */
final class S1ArenaGraph {
    private static final int MAX_DEPTH = 256;
    private S1ArenaGraph() { }
    static List<Integer> order(List<List<Integer>> edges) throws IOException {
        var result = new ArrayList<Integer>();
        int[] heights = new int[edges.size()];
        boolean[] active = new boolean[edges.size()];
        for (int root = 0; root < edges.size(); root++) {
            if (heights[root] != 0) continue;
            var stack = new ArrayList<int[]>();
            stack.add(new int[]{root, 0}); active[root] = true;
            while (!stack.isEmpty()) {
                int[] frame = stack.get(stack.size() - 1);
                if (frame[1] < edges.get(frame[0]).size()) {
                    int child = edges.get(frame[0]).get(frame[1]++);
                    check(!active[child], "Cyclic arena graph");
                    if (heights[child] == 0) {
                        check(stack.size() < MAX_DEPTH, "Graph exceeds depth limit");
                        active[child] = true; stack.add(new int[]{child, 0});
                    }
                } else {
                    int height = 1;
                    for (int child: edges.get(frame[0])) height = Math.max(height, heights[child] + 1);
                    check(height <= MAX_DEPTH, "Graph exceeds depth limit");
                    heights[frame[0]] = height; active[frame[0]] = false;
                    result.add(frame[0]); stack.remove(stack.size() - 1);
                }
            }
        }
        return result;
    }

    private static void check(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }
}

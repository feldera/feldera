package org.dbsp.sqlCompiler.compiler.backend.dot;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitTransform;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/** Dump a graph in the graphviz dot format */
public class ToDot {
    public static void customDump(String fileName, @Nullable String outputFormat,
                                  DBSPCircuit circuit,
                                  ToDotEdgesVisitor.VisitorConstructor nodesVisitor,
                                  ToDotEdgesVisitor.VisitorConstructor edgesVisitor) {
        if (circuit.isEmpty())
            return;
        File tmp = null;
        try {
            tmp = File.createTempFile("tmp", ".dot");
            tmp.deleteOnExit();
            PrintWriter writer = new PrintWriter(tmp.getAbsolutePath());
            IndentStream stream = new IndentStream(writer);
            stream.append("digraph ")
                    .append(circuit.id)
                    .append(" {")
                    .increase()
                    .append("ordering=\"in\"").newline();
            CircuitVisitor nVisitor = nodesVisitor.create(stream);
            CircuitVisitor eVisitor = edgesVisitor.create(stream);
            nVisitor.apply(circuit);
            eVisitor.apply(circuit);
            stream.decrease().append("}").newline();
            writer.close();
            if (outputFormat != null) {
                System.out.println("Writing " + fileName + " " + circuit.id);
                Utilities.runProcess(".", "dot", "-T", outputFormat,
                        "-o", fileName, tmp.getAbsolutePath());
            }
        } catch (Exception ex) {
            if (tmp != null) {
                try {
                    System.out.println(Utilities.readFile(tmp.toPath()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            throw new RuntimeException(ex);
        }
    }

    public static void dump(DBSPCompiler compiler, String fileName, int details,
                            @Nullable String outputFormat, DBSPCircuit circuit) {
        customDump(fileName, outputFormat, circuit,
                stream -> new ToDotNodesVisitor(compiler, stream, details),
                stream -> new ToDotEdgesVisitor(compiler, stream, details));
    }

    static int counter = 0;

    /** Returns a circuit transform which can be inserted in the CircuitOptimizer to dump the
     * circuit at some point */
    public static CircuitTransform dumper(DBSPCompiler compiler, String file, int details) {
        return new CircuitTransform() {
            @Override
            public String getName() {
                return "toDot";
            }

            @Override
            public DBSPCircuit apply(DBSPCircuit circuit) {
                ToDot.dump(compiler, counter++ + file, details, "png", circuit);
                return circuit;
            }

            @Override
            public String toString() {
                return "toDot";
            }
        };
    }
}

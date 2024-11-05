package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.ToDotEdgesVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Logger;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/** Dump a graph in the graphviz dot format */
public class ToDot {
    public static void dump(String fileName, @Nullable String outputFormat,
                            DBSPCircuit circuit,
                            ToDotEdgesVisitor.VisitorConstructor nodesVisitor,
                            ToDotEdgesVisitor.VisitorConstructor edgesVisitor) {
        if (circuit.isEmpty())
            return;
        System.out.println("Writing circuit to " + fileName);
        Logger.INSTANCE.belowLevel("ToDotVisitor", 1)
                .append("Writing circuit to ")
                .append(fileName)
                .newline();
        File tmp = null;
        try {
            tmp = File.createTempFile("tmp", ".dot");
            tmp.deleteOnExit();
            PrintWriter writer = new PrintWriter(tmp.getAbsolutePath());
            IndentStream stream = new IndentStream(writer);
            stream.append("digraph ")
                    .append(circuit.name)
                    .append(" {")
                    .append("ordering=\"in\"").newline()
                    .increase();
            CircuitVisitor nVisitor = nodesVisitor.create(stream);
            CircuitVisitor eVisitor = edgesVisitor.create(stream);
            nVisitor.apply(circuit);
            eVisitor.apply(circuit);
            stream.decrease().append("}").newline();
            writer.close();
            if (outputFormat != null)
                Utilities.runProcess(".", "dot", "-T", outputFormat,
                        "-o", fileName, tmp.getAbsolutePath());
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

    public static void dump(IErrorReporter reporter, String fileName, int details,
                            @Nullable String outputFormat, DBSPCircuit circuit) {
        dump(fileName, outputFormat, circuit,
                stream -> new ToDotNodesVisitor(reporter, stream, details),
                stream -> new ToDotEdgesVisitor(reporter, stream, details));
    }
}

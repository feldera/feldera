package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Generates documentation for the functions implemented */
public class FunctionDocumentation {
    public interface FunctionDescription {
        /** Name of function */
        String functionName();
        /** File name containing documentation */
        String documentation();
        /** True if function is an aggregate */
        boolean aggregate();
    }

    public interface FunctionRegistry {
        List<FunctionDescription> getDescriptions();
    }

    /** Generate documentation with an index of all functions supported */
    public static void generateIndex(String file) throws IOException {
        File f = new File(file);
        File dir = f.getParentFile();
        Set<String> docFiles = new HashSet<>(Linq.list(Objects.requireNonNull(dir.list())));

        PrintWriter writer = new PrintWriter(f.getAbsolutePath());
        writer.println("# Index of functions and SQL constructs supported in Feldera SQL");
        writer.println();
        List<FunctionDescription> sorted = new ArrayList<>();
        sorted.addAll(CalciteFunctions.INSTANCE.getDescriptions());
        sorted.addAll(PredefinedFunctions.INSTANCE.getDescriptions());
        sorted.sort(Comparator.comparing(FunctionDescription::functionName));
        Map<String, String> fileContents = new HashMap<>();

        FunctionDescription previous = null;
        for (FunctionDescription func : sorted) {
            if (func.documentation().isEmpty())
                continue;
            if (previous != null && func.functionName().equals(previous.functionName()))
                continue;
            previous = func;
            String[] files = func.documentation().split(",");
            writer.print("* `" + func.functionName() + "`");
            if (func.aggregate())
                writer.print(" (aggregate)");
            writer.print(": ");
            boolean first = true;
            for (String doc: files) {
                String docFile = doc + ".md";
                if (!docFiles.contains(docFile))
                    // Check that the file exists
                    throw new RuntimeException("File `" + docFile + "` not found for function " + func.functionName());
                if (!fileContents.containsKey(docFile)) {
                    // Cache the file's contents
                    String contents = Utilities.readFile(Paths.get(dir.getPath(), docFile));
                    Utilities.putNew(fileContents, docFile, contents);
                }
                String contents = Utilities.getExists(fileContents, docFile);
                if (!contents.contains(func.functionName()))
                    // Check that the file does indeed mention this function
                    throw new RuntimeException("Function `" + func.functionName() + "` does not appear in file " + docFile);
                if (!first)
                    writer.print(", ");
                first = false;
                writer.print("[" + doc + "](" + docFile + ")");
            }
            writer.println();
        }
        writer.close();
    }
}

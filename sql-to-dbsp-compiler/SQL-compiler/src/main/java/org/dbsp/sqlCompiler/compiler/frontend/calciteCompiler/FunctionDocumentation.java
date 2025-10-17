package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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

    private static void writeFunction(
            File dir,
            // A list of functions that share the same name.
            List<FunctionDescription> funcs,
            PrintWriter writer,
            Map<String, String> fileContents, Set<String> docFiles) throws IOException {
        Utilities.enforce(!funcs.isEmpty());
        FunctionDescription func = funcs.get(0);
        List<String> files = new ArrayList<>();
        for (FunctionDescription f: funcs) {
            String[] fs = func.documentation().split(",");
            files.addAll(Arrays.asList(fs));
        }
        writer.print("* `" + func.functionName().toUpperCase(Locale.ENGLISH) + "`");
        if (func.aggregate())
            writer.print(" (aggregate)");
        writer.print(": ");
        boolean first = true;
        Set<String> anchorsPrinted = new HashSet<>();
        for (String doc: files) {
            String anchor = "";
            String docFile = doc;
            if (doc.contains("#")) {
                anchor = doc.substring(doc.indexOf("#") + 1);
                docFile = docFile.substring(0, doc.indexOf("#"));
            }
            docFile = docFile + ".md";
            if (!docFiles.contains(docFile))
                // Check that the file exists
                throw new RuntimeException("File `" + docFile + "` not found for function " + func.functionName());
            if (!fileContents.containsKey(docFile)) {
                // Cache the file's contents
                String contents = Utilities.readFile(Paths.get(dir.getPath(), docFile));
                Utilities.putNew(fileContents, docFile, contents);
            }
            String contents = Utilities.getExists(fileContents, docFile);
            String funcName = func.functionName().toUpperCase(Locale.ENGLISH);
            if (!contents.contains(funcName)) {
                // Check that the file does indeed mention this function
                if (funcName.contains(" "))
                    // These don't appear ad-literam, e.g., NOT IN, EXCEPT ALL
                    funcName = funcName.substring(0, funcName.indexOf(" "));
                if (!contents.contains(funcName))
                    throw new RuntimeException("Function `" + func.functionName() + "` does not appear in file " + docFile);
            }
            if (!anchor.isEmpty()) {
                if (!contents.contains("<a id=\"" + anchor + "\"></a>")) {
                    // Check that the file contains an anchor.
                    // It can be either <a id="anchor"> or # anchor
                    if (!contents.toLowerCase().replace("`", "")
                            .contains("# " + anchor.replace("-", " ")))
                        throw new RuntimeException("Anchor `" + anchor + "` does not appear in file " + docFile);
                }
                anchor = "#" + anchor;
            } else {
                throw new RuntimeException("No anchor for function " +
                        Utilities.singleQuote(func.functionName()) + " in file " + Utilities.singleQuote(doc));
            }
            String toPrint = docFile + anchor;
            if (anchorsPrinted.contains(toPrint))
                continue;
            if (!first)
                writer.print(", ");
            first = false;
            anchorsPrinted.add(toPrint);
            writer.print("[" + Utilities.getBaseName(docFile) + "](" + toPrint + ")");
        }
        writer.println();
    }

    static boolean sameFunction(FunctionDescription left, FunctionDescription right) {
        return left.aggregate() == right.aggregate() && left.functionName().equals(right.functionName());
    }

    /** Generate documentation with an index of all functions supported */
    public static void generateIndex(String file) throws IOException {
        File f = new File(file);
        File dir = f.getParentFile();
        Set<String> docFiles = new HashSet<>(Linq.list(Objects.requireNonNull(dir.list())));

        PrintWriter writer = new PrintWriter(f.getAbsolutePath());
        writer.println("# Index of Functions and SQL Constructs Supported in Feldera SQL");
        writer.println();
        List<FunctionDescription> sorted = new ArrayList<>();
        sorted.addAll(CalciteFunctions.INSTANCE.getDescriptions());
        sorted.addAll(new CustomFunctions().getDescriptions());
        sorted.sort(Comparator.comparing(FunctionDescription::functionName, String.CASE_INSENSITIVE_ORDER));
        Map<String, String> fileContents = new HashMap<>();

        FunctionDescription previous = null;
        List<FunctionDescription> funcs = new ArrayList<>();
        for (FunctionDescription func : sorted) {
            if (func.documentation().isEmpty())
                continue;
            if (previous != null && !sameFunction(previous, func)) {
                writeFunction(dir, funcs, writer, fileContents, docFiles);
                funcs.clear();
            }
            previous = func;
            funcs.add(func);
        }
        writeFunction(dir, funcs, writer, fileContents, docFiles);
        writer.close();
    }
}

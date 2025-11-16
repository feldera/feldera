package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
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
    // TODO: remove this once https://github.com/feldera/feldera/issues/5041 is resolved
    static final String NO_FILE = "nofile.py";

    public interface FunctionDescription {
        /** Name of function */
        String functionName();
        /** File name containing documentation */
        String documentation();
        /** True if function is an aggregate */
        boolean aggregate();
        /** Python file which tests this function */
        String testedBy();
    }

    public interface FunctionRegistry {
        List<FunctionDescription> getDescriptions();
    }

    /** Maps file names to file contents */
    static class TextFileCache {
        final Map<String, String> contents;

        TextFileCache() {
            this.contents = new HashMap<>();
        }

        String getContents(String fileName) throws IOException {
            if (!this.contents.containsKey(fileName)) {
                // Cache the file's contents
                String contents = Utilities.readFile(fileName);
                Utilities.putNew(this.contents, fileName, contents);
                return contents;
            }
            return Utilities.getExists(this.contents, fileName);
        }
    }

    private static void writeFunction(
            File dir,
            // A list of functions that share the same name.
            List<FunctionDescription> funcs,
            PrintWriter writer,
            TextFileCache fileContents, Set<String> docFiles) throws IOException {
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
            Path path = Paths.get(dir.getPath(), docFile);
            String contents = fileContents.getContents(path.toString());
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

    static final String PYTHON_TESTS = "../../python/tests";

    static void checkTestedBy(FunctionDescription description, TextFileCache fileContents) throws IOException {
        // A list of file names separated by pipe symbols
        String[] patterns = description.testedBy().replace("\n", "").split("\\|");
        Path pythonTests = Paths.get(PYTHON_TESTS);
        for (String pattern: patterns) {
            if (pattern.equalsIgnoreCase("nofile.py"))
                // TODO: remove this path once test information has been supplied for all functions
                continue;
            for (String fileName: Utilities.expandBraces(pattern)) {
                Path path = pythonTests.resolve(fileName);
                File file = path.toFile();
                if (!file.exists())
                    throw new RuntimeException("File " + file.getPath() + " does not exist");
                String contents = fileContents.getContents(file.getPath());
                if (!description.functionName().isEmpty() &&
                        !contents.toLowerCase().contains(description.functionName().toLowerCase()))
                    throw new RuntimeException("Could not find function " + description.functionName() + " in " + file.getPath());
            }
        }
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
        TextFileCache fileContents = new TextFileCache();

        FunctionDescription previous = null;
        List<FunctionDescription> funcs = new ArrayList<>();
        for (FunctionDescription func : sorted) {
            checkTestedBy(func, fileContents);
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

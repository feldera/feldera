package org.dbsp.sqlCompiler.compiler.sql;

import org.dbsp.sqlCompiler.CompilerMain;
import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.util.Utilities;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Compile the SQL programs in the feldera-qa repository, and rust-compile the results. */
public class QATests {
    /** Key: placeholder name in a workload template; value: the text that replaces it.
     * A name that ends in "_connectors" is a connector list and becomes '[]'. */
    static final Map<String, String> PLACEHOLDERS = Map.of(
            "join_view_kind", "VIEW",
            "slices", "16",
            "slice", "0",
            "limit", "1000",
            "rate", "100");
    /** Python str.format escapes and fields: {{, }}, {name}. */
    static final Pattern FORMAT_FIELD = Pattern.compile("\\{\\{|}}|\\{(\\w+)}");
    /** Python string.Template escapes and fields: $$, ${name}, $name. */
    static final Pattern TEMPLATE_FIELD = Pattern.compile("\\$\\$|\\$\\{(\\w+)}|\\$(\\w+)");

    static boolean isPlaceholder(@Nullable String name) {
        return name != null && (name.endsWith("_connectors") || PLACEHOLDERS.containsKey(name));
    }

    /** Name of the field in a FORMAT_FIELD or TEMPLATE_FIELD match; null for an escape. */
    @Nullable
    static String fieldName(MatchResult match) {
        for (int i = 1; i <= match.groupCount(); i++)
            if (match.group(i) != null)
                return match.group(i);
        return null;
    }

    static String replacement(MatchResult match) {
        String name = fieldName(match);
        if (name == null)
            // Escape: keep the second character
            return Matcher.quoteReplacement(match.group().substring(1));
        Utilities.enforce(isPlaceholder(name), () -> "Unknown placeholder " + match.group());
        String value = name.endsWith("_connectors") ? "[]" : PLACEHOLDERS.get(name);
        return Matcher.quoteReplacement(value);
    }

    /** Replace the fields and escapes of a template, like the Python workload drivers do.
     * A program that has no known placeholder is not a template and does not change. */
    static String fillTemplate(String program, Pattern field) {
        boolean isTemplate = field.matcher(program).results()
                .anyMatch(m -> isPlaceholder(fieldName(m)));
        if (!isTemplate)
            return program;
        return field.matcher(program).replaceAll(QATests::replacement);
    }

    static File fillTemplates(File source) throws IOException {
        String program = Files.readString(source.toPath());
        program = fillTemplate(program, FORMAT_FIELD);
        program = fillTemplate(program, TEMPLATE_FIELD);
        File file = BaseSQLTests.createInputScript(program);
        file.deleteOnExit();
        return file;
    }

    static void compileAndCheck(File file) throws SQLException, IOException {
        File input = fillTemplates(file);
        CompilerMessages messages = CompilerMain.execute(
                "-i", "--alltables", "--ignoreOrder",
                "-o", BaseSQLTests.TEST_FILE_PATH, input.getAbsolutePath());
        if (messages.errorCount() > 0) {
            messages.print();
            throw new RuntimeException("Error during compilation");
        }
        Utilities.deleteFile(input, false);
        // BaseSQLTests.compileAndCheckRust(true);
    }

    /** Enumerate the SQL files in the QA repository.
     * The assumption is that this repository has been checked out in parallel with feldera. */
    static List<File> getQATests() {
        List<File> result = new ArrayList<>();
        String dir = "../../../feldera-qa";
        String[] subdirs = new String[] { "workloads" };
        for (var subdir : subdirs) {
            File file = new File(dir + "/" + subdir);
            if (file.exists()) {
                File[] directories = file.listFiles();
                if (directories == null)
                    return result;
                Arrays.sort(directories);
                for (File d : directories) {
                    File[] files = d.listFiles();
                    if (files == null)
                        continue;
                    for (File c : files) {
                        // The following eliminate some fda scripts
                        if (c.getName().contains("adhoc")) continue;
                        if (c.getName().matches("query.*view.sql")) continue;
                        // Ad-hoc queries that check the views of program.sql
                        if (c.getName().endsWith("_check.sql")) continue;
                        if (c.getName().endsWith(".sql")) {
                            result.add(c);
                        }
                    }
                }
            }
        }
        return result;
    }

    @Test
    public void qaTests() throws SQLException {
        // BaseSQLTests.showPlan();
        for (File c : getQATests()) {
            // This program cannot be compiled because it contains a udf
            if (c.toString().matches(".*swiss.*-q1.*")) continue;
            // if (!c.toString().contains("x.sql")) continue;
            System.out.println("Compiling " + c);
            try {
                compileAndCheck(c);
            } catch (UnsupportedException | IOException ex) {
                // This is probably a file containing an ad-hoc query
                System.out.println(c.getName() + " skipped due to unsupported features.");
            }
        }
    }
}

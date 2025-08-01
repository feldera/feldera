package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.util.Utilities;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TestUtil {
    /** Read the contents of a test resource file and return it as a string.
     * @param filename Path under test/resources */
    public static String readStringFromResourceFile(String filename) throws IOException {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        try (InputStream is = TestUtil.class.getClassLoader().getResourceAsStream(filename);
             InputStreamReader streamReader = new InputStreamReader(Objects.requireNonNull(is), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!first)
                    result.append("\n");
                first = false;
                result.append(line);
            }
        }
        return result.toString();
    }

    /** Enumerate all files in the test resources directory */
    public static List<String> enumerateResourceFiles() throws URISyntaxException {
        List<String> result = new ArrayList<>();
        URL dirURL = Thread.currentThread().getContextClassLoader().getResource(".");
        if (dirURL != null && dirURL.getProtocol().equals("file")) {
            File folder = new File(dirURL.toURI());
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    result.add(file.getName());
                }
            }
        }
        return result;
    }

    /** Check that the messages contain the specified substring.
     * @param messages  Compiler messages.
     * @param contents  Substring that we expect to find. */
    public static void assertMessagesContain(CompilerMessages messages, String contents) {
        String text = messages.toString();
        if (text.contains(contents))
            return;
        System.out.println(text);
        Assert.fail("Expected message to contain " + Utilities.singleQuote(contents));
    }

    /** Check that the messages contain the specified substring.
     * @param compiler  Compiler.
     * @param contents  Substring that we expect to find. */
    public static void assertMessagesContain(DBSPCompiler compiler, String contents) {
        compiler.runAllCompilerStages();
        CompilerMessages messages = compiler.messages;
        assertMessagesContain(messages, contents);
    }
}

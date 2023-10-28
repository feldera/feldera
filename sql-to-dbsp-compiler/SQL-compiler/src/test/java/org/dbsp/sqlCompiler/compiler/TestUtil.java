package org.dbsp.sqlCompiler.compiler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class TestUtil {
    /**
     * Read the contents of a test resource file and return it as a string.
     * @param filename Path under test/resources
     */
    public static String readStringFromResourceFile(String filename) throws IOException {
        StringBuilder result = new StringBuilder();
        try (InputStream is = TestUtil.class.getClassLoader().getResourceAsStream(filename);
             InputStreamReader streamReader = new InputStreamReader(Objects.requireNonNull(is), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {
            String line;
            while ((line = reader.readLine()) != null)
                result.append(line).append("\n");
        }
        return result.toString();
    }
}

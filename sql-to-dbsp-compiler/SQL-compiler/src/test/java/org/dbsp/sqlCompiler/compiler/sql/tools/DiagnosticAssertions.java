package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Assert;

import java.util.List;
import java.util.regex.Pattern;

/** Preserve direct diagnostic assertions that bypass the compilation helpers.
 * Targets describe the inspected value; rendered text is not automatically a
 * dependency on every message body emitted by that compilation. */
public final class DiagnosticAssertions {
    private DiagnosticAssertions() { }

    public enum Target {
        MESSAGE_BODY, ERROR_CLASS, RENDERED, RENDERED_WARNING_LINES,
        SOURCE, SNAPSHOT, REGEX_EXPLANATION, CONFIG_EXCEPTION, STRUCTURED_WARNINGS, STRUCTURED_ERRORS
    }

    public static void assertEquals(Target target, String expected, String actual) {
        Assert.assertEquals(expected, actual);
        DiagnosticExporter.recordDirect(target, "equals", expected, actual);
    }

    public static void assertEquals(Target target, String context, String expected, String actual) {
        Assert.assertEquals(context, expected, actual);
        DiagnosticExporter.recordDirect(target, "equals", expected, actual, context);
    }

    public static void assertListEquals(Target target, List<String> expected, List<String> actual) {
        Assert.assertEquals(expected, actual);
        DiagnosticExporter.recordDirect(target, "ordered_list_equals", expected, actual);
    }

    public static void assertSnapshotEquals(JsonNode expected, JsonNode actual) {
        Assert.assertEquals(expected, actual);
        DiagnosticExporter.recordDirect(Target.SNAPSHOT, "equals", expected, actual);
    }

    public static void assertContains(Target target, String expected, String actual) {
        Assert.assertTrue(actual, actual.contains(expected));
        DiagnosticExporter.recordDirect(target, "contains", expected, actual);
    }

    public static void assertContains(Target target, String context, String expected, String actual) {
        Assert.assertTrue(context, actual.contains(expected));
        DiagnosticExporter.recordDirect(target, "contains", expected, actual, context);
    }

    public static void assertStartsWith(Target target, String expected, String actual) {
        Assert.assertTrue(actual, actual.startsWith(expected));
        DiagnosticExporter.recordDirect(target, "starts_with", expected, actual);
    }

    public static void assertNotContains(Target target, String unexpected, String actual) {
        Assert.assertFalse(actual, actual.contains(unexpected));
        DiagnosticExporter.recordDirect(target, "not_contains", unexpected, actual);
    }

    public static void assertNoMatch(Target target, String regex, String actual) {
        Assert.assertFalse(actual, Pattern.compile(regex).matcher(actual).find());
        DiagnosticExporter.recordDirect(target, "no_regex_find", regex, actual);
    }
}

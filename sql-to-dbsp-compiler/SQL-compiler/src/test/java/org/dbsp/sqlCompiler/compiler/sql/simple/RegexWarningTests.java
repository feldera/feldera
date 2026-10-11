package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.errors.CompilerMessages;
import org.dbsp.sqlCompiler.compiler.frontend.RustRegexChecker;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Tests for {@link RustRegexChecker} */
public class RegexWarningTests extends BaseSQLTests {
    /** Maps each pattern that the Rust regex crate 1.12.3 rejects to the description
     * that the checker gives */
    static final Map<String, String> REJECTED = Map.ofEntries(
            Map.entry("a(?=b)", "the look-ahead '(?='"),
            Map.entry("a(?!b)", "the look-ahead '(?!'"),
            Map.entry("(?<=a)b", "the look-behind '(?<='"),
            Map.entry("(?<!a)b", "the look-behind '(?<!'"),
            Map.entry("(a)\\1", "the backreference or octal escape '\\1'"),
            Map.entry("(a)\\9", "the backreference or octal escape '\\9'"),
            Map.entry("\\0", "the backreference or octal escape '\\0'"),
            Map.entry("\\07", "the backreference or octal escape '\\0'"),
            Map.entry("(?<n>a)\\k<n>", "the escape sequence '\\k'"),
            Map.entry("(?P<n>a)(?P=n)", "the group syntax '(?P='"),
            Map.entry("(?P>n)", "the group syntax '(?P>'"),
            Map.entry("(a)\\g1", "the escape sequence '\\g'"),
            Map.entry("\\Qa\\E", "the escape sequence '\\Q'"),
            Map.entry("\\G", "the escape sequence '\\G'"),
            Map.entry("\\Z", "the escape sequence '\\Z'"),
            Map.entry("\\R", "the escape sequence '\\R'"),
            Map.entry("\\X", "the escape sequence '\\X'"),
            Map.entry("\\h", "the escape sequence '\\h'"),
            Map.entry("\\K", "the escape sequence '\\K'"),
            Map.entry("\\e", "the escape sequence '\\e'"),
            Map.entry("\\cA", "the escape sequence '\\c'"),
            Map.entry("\\o{7}", "the escape sequence '\\o'"),
            Map.entry("(?>a)", "the atomic group '(?>'"),
            Map.entry("(?|a)", "the branch reset group '(?|'"),
            Map.entry("(?#c)", "the comment group '(?#'"),
            Map.entry("(?(1)a|b)", "the conditional group '(?('"),
            Map.entry("(?1)", "the group syntax '(?1'"),
            Map.entry("(?&n)", "the group syntax '(?&'"),
            Map.entry("(?'n'a)", "the group syntax '(?''"),
            Map.entry("(?d)a", "the group syntax '(?d'"),
            Map.entry("(?i-d:a)", "the group syntax '(?i-d'"),
            Map.entry("[a\\Q]", "the escape sequence '\\Q'"),
            Map.entry("[\\1]", "the backreference or octal escape '\\1'"),
            Map.entry("\\\\(?=a)", "the look-ahead '(?='"),
            Map.entry("a\\", "a trailing backslash"),
            Map.entry("\\é", "the escape sequence '\\é'"),
            Map.entry("(a", "an unclosed group"),
            Map.entry("a)", "an unmatched ')'"),
            Map.entry("[a", "an unclosed character class"),
            Map.entry("[]", "an unclosed character class"),
            Map.entry("[^]", "an unclosed character class"),
            Map.entry("[a[]]", "an unclosed character class"),
            Map.entry("((a)", "an unclosed group"),
            Map.entry("(a))", "an unmatched ')'"),
            Map.entry("*a", "the quantifier '*' with nothing to repeat"),
            Map.entry("a|*b", "the quantifier '*' with nothing to repeat"),
            Map.entry("(*a)", "the quantifier '*' with nothing to repeat"),
            Map.entry("(+a)", "the quantifier '+' with nothing to repeat"),
            Map.entry("?a", "the quantifier '?' with nothing to repeat"),
            Map.entry("a|?b", "the quantifier '?' with nothing to repeat"),
            Map.entry("{a", "the quantifier '{' with nothing to repeat"),
            Map.entry("{}", "the quantifier '{' with nothing to repeat"),
            Map.entry("a|{", "the quantifier '{' with nothing to repeat"),
            Map.entry("(?i)*a", "the quantifier '*' with nothing to repeat"),
            Map.entry("a(?i)*", "the quantifier '*' with nothing to repeat"),
            Map.entry("(?:*a)", "the quantifier '*' with nothing to repeat"),
            Map.entry("(?<n>*a)", "the quantifier '*' with nothing to repeat"),
            Map.entry("a{,3}", "the malformed counted repetition '{,3}'"),
            Map.entry("a{", "the malformed counted repetition '{'"),
            Map.entry("a{2", "the malformed counted repetition '{2'"),
            Map.entry("a{,}", "the malformed counted repetition '{,}'"),
            Map.entry("a{3,2}", "the counted repetition '{3,2}', whose minimum exceeds its maximum"),
            Map.entry("a{-1}", "the malformed counted repetition '{-1}'"),
            Map.entry("a{1a}", "the malformed counted repetition '{1a}'"),
            Map.entry("a{b}", "the malformed counted repetition '{b}'"),
            Map.entry("a{2}{", "the malformed counted repetition '{'"),
            Map.entry("a{99999999999}", "the malformed counted repetition '{99999999999}'"),
            Map.entry("(?", "an unclosed group"),
            Map.entry("(?i", "an unclosed group"),
            Map.entry("(?<n", "an unclosed group"),
            Map.entry("(?P", "an unclosed group"),
            Map.entry("(?P<n>", "an unclosed group"));

    /** Patterns that the Rust regex crate 1.12.3 accepts */
    static final List<String> ACCEPTED = List.of(
            "(?R)", "(?R)a", "a*+", "a++", "a{2}+", "(?U)a+", "(?i-s:a)", "(?:a)", "(?P<n>a)", "(?<n>a)",
            "(?<first_name>a)", "[a&&b]", "[a--b]", "[a~~b]", "[[:alpha:]]", "[[:alpha:](?=]", "[(?=]",
            "[]a(?=]", "[^](?=]", "[a[b](?=]", "\\p{IsGreek}", "\\pL", "\\p{Script=Greek}", "\\x41",
            "\\x{263a}", "☺", "\\U0001F600", "\\b{start}", "\\<a\\>", "\\Aa\\z", "\\.\\-\\ \\#\\&\\~",
            "\\(?=a\\)", "(?x)a # \\Q comment", "\\\\", "a\\tb\\n\\r\\f\\v\\a", "\\d\\D\\s\\S\\w\\W\\b\\B",
            "a**", "a{2}{3}", "a{ 2}", "a{2 }", "a{2, 3}", "a{2 ,3}", "a{02}", "a{2,}", "a{2}}", "a}",
            "a{2}?", "\\x{41}{2}", "\\p{L}{2}", "\\x41{2}", "\\b{2}", "a\\b{2}", "\\b{ 2}", "[a[]b]]",
            "[(]", "[)]", "[{]", "[*]", "\\(", "a\\)", "^*", "$+", "a|", "|a", "(a|)", "a||b", "(|a)",
            "()", "(?i)a*", "(?i:a)*", "(?<n>a{2})", "x{2}", "a{0000000000002}", "\\u263a", "\\u{263a}");

    @Test
    public void rejectedPatterns() {
        for (Map.Entry<String, String> entry : REJECTED.entrySet())
            Assert.assertEquals(entry.getKey(), entry.getValue(),
                    RustRegexChecker.unsupportedConstruct(entry.getKey()));
    }

    @Test
    public void acceptedPatterns() {
        for (String pattern : ACCEPTED)
            Assert.assertNull(pattern, RustRegexChecker.unsupportedConstruct(pattern));
    }

    /** The regular expression warnings for the program, one line per warning */
    List<String> warnings(String program) {
        var cc = this.getCC("CREATE TABLE T(s VARCHAR, p VARCHAR);\n" + program);
        List<String> result = new ArrayList<>();
        for (CompilerMessages.Message message : cc.compiler.messages.messages) {
            if (!message.errorType.equals(RustRegexChecker.WARNING))
                continue;
            for (String line : message.toString().split("\n"))
                if (line.contains("warning: "))
                    result.add(line.replace("(no input file):", ""));
        }
        return result;
    }

    @Test
    public void warningRendered() {
        var cc = this.getCC("CREATE TABLE T(s VARCHAR);\nCREATE VIEW V AS SELECT s RLIKE 'a(?=b)' FROM T;");
        StringBuilder output = new StringBuilder();
        for (CompilerMessages.Message message : cc.compiler.messages.messages)
            if (message.errorType.equals(RustRegexChecker.WARNING))
                output.append(message);
        Assert.assertEquals("""
                While compiling:
                    1|CREATE TABLE T(s VARCHAR);
                    2|CREATE VIEW V AS SELECT s RLIKE 'a(?=b)' FROM T;
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                (no input file): Unsupported regular expression feature
                (no input file):2:25: warning: Unsupported regular expression feature: Pattern 'a(?=b)' contains the look-ahead '(?='
                    1|CREATE TABLE T(s VARCHAR);
                    2|CREATE VIEW V AS SELECT s RLIKE 'a(?=b)' FROM T;
                                              ^^^^^^^^^^^^^^^^""", output.toString().stripTrailing());
    }

    @Test
    public void everyRegexFunction() {
        List<String> warnings = this.warnings("""
                CREATE VIEW V AS SELECT
                   s RLIKE '(?=a)',
                   s NOT RLIKE '(?!a)',
                   RLIKE(s, '(?<=a)'),
                   REGEXP_REPLACE(s, '(?<!a)'),
                   REGEXP_REPLACE(s, '\\1', 'x')
                FROM T;""");
        Assert.assertEquals(List.of(
                "3:4: warning: Unsupported regular expression feature: Pattern '(?=a)' contains the look-ahead '(?='",
                "4:4: warning: Unsupported regular expression feature: Pattern '(?!a)' contains the look-ahead '(?!'",
                "5:4: warning: Unsupported regular expression feature: Pattern '(?<=a)' contains the look-behind '(?<='",
                "6:4: warning: Unsupported regular expression feature: Pattern '(?<!a)' contains the look-behind '(?<!'",
                "7:4: warning: Unsupported regular expression feature: Pattern '\\1' contains the backreference or octal escape '\\1'"),
                warnings);
    }

    @Test
    public void noWarnings() {
        // Valid constant patterns, patterns computed at runtime, and NULL patterns
        List<String> warnings = this.warnings("""
                CREATE VIEW V AS SELECT
                   s RLIKE '(?P<n>a)\\d+',
                   s RLIKE p,
                   s RLIKE NULL,
                   REGEXP_REPLACE(s, p, 'x')
                FROM T;""");
        Assert.assertEquals(List.of(), warnings);
    }
}

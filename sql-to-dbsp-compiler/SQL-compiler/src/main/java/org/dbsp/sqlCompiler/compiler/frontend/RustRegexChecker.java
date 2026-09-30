package org.dbsp.sqlCompiler.compiler.frontend;

import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.regex.Pattern;

/** Best-effort detection of the constructs that the Rust {@code regex} crate rejects
 * in a regular expression.
 *
 * <p>The checker reports a construct only when the Rust parser rejects it for sure:
 * look-around, backreferences, escapes and group forms that Rust does not define,
 * unbalanced groups and character classes, quantifiers with nothing to repeat, and
 * malformed counted repetitions.  It does not detect other invalid patterns, such as
 * invalid class ranges, unknown Unicode classes, or patterns that exceed the Rust
 * size limit.  It does not analyze patterns that enable verbose mode. */
public final class RustRegexChecker {
    /** Name of the warning; silenced by {@code SET FELDERA_IGNORE_WARNING_UNSUPPORTED_REGULAR_EXPRESSION_FEATURE = ON} */
    public static final String WARNING = "Unsupported regular expression feature";

    /** Letters that the Rust parser accepts after a backslash */
    static final String ESCAPE_LETTERS = "aftnrvxuUpPdDsSwWAzbB";
    /** Escapes whose argument may be enclosed in braces, as in {@code \x{263a}} or {@code \p{Greek}} */
    static final String BRACED_ESCAPES = "xuUpP";
    /** Flags that the Rust parser accepts in a group such as {@code (?i-s:...)} */
    static final String FLAGS = "imsUuxR-";
    /** A group that may enable verbose mode, where whitespace is ignored and {@code #}
     * starts a comment; the checker does not model verbose mode */
    static final Pattern VERBOSE_FLAG = Pattern.compile("\\(\\?[a-zA-Z-]*x");
    /** The largest count that the Rust parser accepts in a counted repetition */
    static final BigInteger MAX_COUNT = BigInteger.valueOf(0xFFFFFFFFL);
    static final String UNCLOSED_GROUP = "an unclosed group";
    static final String UNCLOSED_CLASS = "an unclosed character class";

    private final String pattern;
    /** Index of the next character to scan */
    private int index = 0;
    /** Number of character classes that enclose the next character */
    private int classDepth = 0;
    /** Number of groups that enclose the next character */
    private int groupDepth = 0;
    /** True when the next character starts an expression */
    private boolean atExpressionStart = true;
    /** True when the checker cannot follow the pattern and gives no verdict */
    private boolean lost = false;

    private RustRegexChecker(String pattern) {
        this.pattern = pattern;
    }

    /** Describes the first construct in {@code pattern} that the Rust parser rejects
     * for sure, or returns null if the checker finds no such construct */
    @Nullable
    public static String unsupportedConstruct(String pattern) {
        if (VERBOSE_FLAG.matcher(pattern).find())
            return null;
        return new RustRegexChecker(pattern).scan();
    }

    /** The warning text for a pattern that contains the unsupported {@code construct} */
    public static String message(String pattern, String construct) {
        return "Pattern " + Utilities.singleQuote(pattern) + " contains " + construct;
    }

    @Nullable
    String scan() {
        while (this.index < this.pattern.length() && !this.lost) {
            String problem = this.classDepth > 0 ? this.scanInClass() : this.scanOutsideClass();
            if (problem != null)
                return problem;
        }
        if (this.lost)
            return null;
        if (this.classDepth > 0)
            return UNCLOSED_CLASS;
        if (this.groupDepth > 0)
            return UNCLOSED_GROUP;
        return null;
    }

    char current() {
        return this.pattern.charAt(this.index);
    }

    @Nullable
    String scanInClass() {
        switch (this.current()) {
            case '\\':
                return this.scanEscape();
            case '[':
                this.openClass();
                return null;
            case ']':
                this.classDepth--;
                this.index++;
                return null;
            default:
                this.index++;
                return null;
        }
    }

    @Nullable
    String scanOutsideClass() {
        char current = this.current();
        switch (current) {
            case '\\':
                return this.scanEscape();
            case '[':
                this.openClass();
                this.atExpressionStart = false;
                return null;
            case '(':
                return this.scanGroupOpener();
            case ')':
                if (this.groupDepth == 0)
                    return "an unmatched ')'";
                this.groupDepth--;
                this.atExpressionStart = false;
                this.index++;
                return null;
            case '|':
                this.atExpressionStart = true;
                this.index++;
                return null;
            case '*':
            case '+':
            case '?':
            case '{':
                if (this.atExpressionStart)
                    return "the quantifier " + Utilities.singleQuote(Character.toString(current)) +
                            " with nothing to repeat";
                if (current == '{')
                    return this.scanCountedRepetition();
                this.index++;
                return null;
            default:
                this.atExpressionStart = false;
                this.index++;
                return null;
        }
    }

    /** Scans the escape at the current index */
    @Nullable
    String scanEscape() {
        String problem = this.unsupportedEscape();
        if (problem != null)
            return problem;
        char escaped = this.pattern.charAt(this.index + 1);
        int argument = this.index + 2;
        boolean braced = argument < this.pattern.length() && this.pattern.charAt(argument) == '{';
        if (braced && (BRACED_ESCAPES.indexOf(escaped) >= 0 ||
                (escaped == 'b' && this.isSpecialWordBoundary(argument + 1)))) {
            int close = this.pattern.indexOf('}', argument);
            if (close < 0)
                this.lost = true;
            else
                this.index = close + 1;
        } else {
            this.index = argument;
        }
        this.atExpressionStart = false;
        return null;
    }

    /** True if the braces after {@code \b} hold a name such as {@code start}; otherwise
     * the braces are a counted repetition of {@code \b} */
    boolean isSpecialWordBoundary(int nameStart) {
        if (nameStart >= this.pattern.length())
            return false;
        char first = this.pattern.charAt(nameStart);
        return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '-';
    }

    /** Describes the escape at the current index if the Rust parser rejects it */
    @Nullable
    String unsupportedEscape() {
        if (this.index + 1 >= this.pattern.length())
            return "a trailing backslash";
        char escaped = this.pattern.charAt(this.index + 1);
        String text = Utilities.singleQuote("\\" + escaped);
        if (escaped >= '0' && escaped <= '9')
            return "the backreference or octal escape " + text;
        if (escaped > 127)
            return "the escape sequence " + text;
        if (Character.isLetter(escaped) && ESCAPE_LETTERS.indexOf(escaped) < 0)
            return "the escape sequence " + text;
        return null;
    }

    /** Enters the class that starts at the current index; a {@code ]} right after the
     * {@code [} or {@code [^} is a literal */
    void openClass() {
        this.classDepth++;
        this.index++;
        if (this.index < this.pattern.length() && this.current() == '^')
            this.index++;
        if (this.index < this.pattern.length() && this.current() == ']')
            this.index++;
    }

    /** Enters a group whose body starts at {@code bodyStart} */
    void openGroup(int bodyStart) {
        this.groupDepth++;
        this.index = bodyStart;
        this.atExpressionStart = true;
    }

    /** Scans the group opener at the current index */
    @Nullable
    String scanGroupOpener() {
        int start = this.index;
        if (!this.pattern.startsWith("(?", start)) {
            this.openGroup(start + 1);
            return null;
        }
        String problem = this.unsupportedGroup(start);
        if (problem != null)
            return problem;
        int afterQuestion = start + 2;
        if (afterQuestion >= this.pattern.length())
            return UNCLOSED_GROUP;
        if (this.pattern.startsWith("<", afterQuestion) || this.pattern.startsWith("P<", afterQuestion)) {
            int close = this.pattern.indexOf('>', afterQuestion);
            if (close < 0)
                return UNCLOSED_GROUP;
            this.openGroup(close + 1);
            return null;
        }
        if (this.pattern.charAt(afterQuestion) == 'P')
            // unsupportedGroup rejects every other character after "(?P"
            return UNCLOSED_GROUP;
        int end = this.skipFlags(afterQuestion);
        if (end >= this.pattern.length())
            return UNCLOSED_GROUP;
        if (this.pattern.charAt(end) == ':') {
            this.openGroup(end + 1);
            return null;
        }
        // A group such as (?i) sets flags, and a quantifier cannot repeat it
        this.index = end + 1;
        this.atExpressionStart = true;
        return null;
    }

    /** The index of the first character at or after {@code index} that is not a flag */
    int skipFlags(int index) {
        while (index < this.pattern.length() && FLAGS.indexOf(this.pattern.charAt(index)) >= 0)
            index++;
        return index;
    }

    /** Describes the group that starts at {@code start} with {@code (?} if the Rust parser rejects it */
    @Nullable
    String unsupportedGroup(int start) {
        int index = start + 2;
        if (index >= this.pattern.length())
            return null;
        char first = this.pattern.charAt(index);
        switch (first) {
            case '=':
            case '!':
                return "the look-ahead " + this.quotedPrefix(start, 3);
            case '<': {
                if (this.pattern.startsWith("=", index + 1) || this.pattern.startsWith("!", index + 1))
                    return "the look-behind " + this.quotedPrefix(start, 4);
                return null;
            }
            case 'P': {
                if (index + 1 < this.pattern.length() && this.pattern.charAt(index + 1) != '<')
                    return "the group syntax " + this.quotedPrefix(start, 4);
                return null;
            }
            case '>':
                return "the atomic group " + this.quotedPrefix(start, 3);
            case '#':
                return "the comment group " + this.quotedPrefix(start, 3);
            case '|':
                return "the branch reset group " + this.quotedPrefix(start, 3);
            case '(':
                return "the conditional group " + this.quotedPrefix(start, 3);
            default:
                break;
        }
        index = this.skipFlags(index);
        if (index >= this.pattern.length())
            return null;
        char terminator = this.pattern.charAt(index);
        if (terminator == ':' || terminator == ')')
            return null;
        return "the group syntax " + this.quotedPrefix(start, index + 1 - start);
    }

    /** Scans the counted repetition, such as {@code {2,5}}, that starts at the current index */
    @Nullable
    String scanCountedRepetition() {
        int start = this.index;
        int close = this.pattern.indexOf('}', start);
        String text = close < 0 ? this.pattern.substring(start) : this.pattern.substring(start, close + 1);
        String malformed = "the malformed counted repetition " + Utilities.singleQuote(text);
        if (close < 0)
            return malformed;
        String[] bounds = this.pattern.substring(start + 1, close).split(",", -1);
        if (bounds.length > 2)
            return malformed;
        BigInteger min = parseCount(bounds[0]);
        if (min == null)
            return malformed;
        if (bounds.length == 2 && !strip(bounds[1]).isEmpty()) {
            BigInteger max = parseCount(bounds[1]);
            if (max == null)
                return malformed;
            if (min.compareTo(max) > 0)
                return "the counted repetition " + Utilities.singleQuote(text) +
                        ", whose minimum exceeds its maximum";
        }
        this.index = close + 1;
        return null;
    }

    /** The value of a count in a counted repetition, or null if the Rust parser rejects it */
    @Nullable
    static BigInteger parseCount(String count) {
        String digits = strip(count);
        if (digits.isEmpty())
            return null;
        for (int i = 0; i < digits.length(); i++) {
            char c = digits.charAt(i);
            if (c < '0' || c > '9')
                return null;
        }
        BigInteger value = new BigInteger(digits);
        return value.compareTo(MAX_COUNT) > 0 ? null : value;
    }

    /** Removes the leading and trailing characters that may be Unicode white space;
     * the Rust parser skips white space around each count */
    static String strip(String text) {
        int start = 0;
        int end = text.length();
        while (start < end && maybeWhitespace(text.charAt(start)))
            start++;
        while (end > start && maybeWhitespace(text.charAt(end - 1)))
            end--;
        return text.substring(start, end);
    }

    /** True for every Unicode white space character, and for a few others */
    static boolean maybeWhitespace(char c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\u0085';
    }

    /** The first {@code length} characters of the pattern after {@code start}, quoted */
    String quotedPrefix(int start, int length) {
        int end = Math.min(this.pattern.length(), start + length);
        return Utilities.singleQuote(this.pattern.substring(start, end));
    }
}

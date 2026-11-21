package org.dbsp.sqlCompiler.compiler.sql.tools;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.source.tree.*;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreeScanner;
import com.sun.source.util.Trees;
import org.dbsp.util.Utilities;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Parse source emission expressions without compiling or executing them.
 * Dynamic expressions remain holes, and constructor overloads remain unresolved.
 * Matching these candidates does not establish a reviewed message catalog. */
public final class DiagnosticTemplates {
    private DiagnosticTemplates() { }

    private static final String CONNECTORS = "org.dbsp.sqlCompiler.compiler.frontend.connectors";
    private static final String CONFIG_REPORTER = CONNECTORS + ".ConfigReporter";

    /** Recognize explicitly declared reporter parameters without resolving or executing code.
     * Aliases, fields, and inferred receiver types stay outside this extraction. */
    private static boolean configReporterParameter(CompilationUnitTree unit, VariableTree parameter) {
        String type = parameter.getType().toString();
        if (type.equals(CONFIG_REPORTER)) return true;
        if (!type.equals("ConfigReporter")) return false;
        return CONNECTORS.equals(String.valueOf(unit.getPackageName()))
                || unit.getImports().stream().anyMatch(item -> !item.isStatic()
                && item.getQualifiedIdentifier().toString().equals(CONFIG_REPORTER));
    }

    private static String name(ExpressionTree expression) {
        if (expression instanceof MemberSelectTree member) return member.getIdentifier().toString();
        return expression.toString();
    }

    private static String literal(ExpressionTree expression) {
        return expression instanceof LiteralTree value && value.getValue() instanceof String text ? text : null;
    }

    private static boolean containsString(ExpressionTree expression) {
        if (expression instanceof LiteralTree literal) return literal.getValue() instanceof String;
        if (expression instanceof ParenthesizedTree parentheses) return containsString(parentheses.getExpression());
        return expression instanceof BinaryTree binary && binary.getKind() == Tree.Kind.PLUS
                && (containsString(binary.getLeftOperand()) || containsString(binary.getRightOperand()));
    }

    private static void parts(ExpressionTree expression, ArrayNode output) {
        if (expression instanceof ParenthesizedTree parentheses) {
            parts(parentheses.getExpression(), output);
        } else if (expression instanceof LiteralTree literal && literal.getValue() instanceof String text) {
            output.addObject().put("literal", text);
        } else if (expression instanceof BinaryTree binary && binary.getKind() == Tree.Kind.PLUS
                && containsString(binary)) {
            parts(binary.getLeftOperand(), output);
            parts(binary.getRightOperand(), output);
        } else if (expression instanceof MethodInvocationTree call && call.getArguments().size() == 1
                && call.getMethodSelect().toString().equals("Utilities.singleQuote")) {
            output.addObject().put("literal", "'");
            parts(call.getArguments().getFirst(), output);
            output.addObject().put("literal", "'");
        } else {
            output.addObject().put("dynamic", expression.toString());
        }
    }

    static ObjectNode extract(Path sourceRoot) throws IOException {
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new IOException("A full JDK is required to parse diagnostic sources");
        List<Path> paths;
        try (var files = Files.walk(sourceRoot)) {
            paths = files.filter(path -> path.toString().endsWith(".java")).sorted().toList();
        }
        if (paths.isEmpty()) throw new IOException("No Java source files");
        var result = Utilities.deterministicObjectMapper().createObjectNode();
        result.put("schema_version", 1).put("source_files", paths.size()).put("catalog_reviewed", false);
        var sites = result.putArray("sites");
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var manager = compiler.getStandardFileManager(diagnostics, null, java.nio.charset.StandardCharsets.UTF_8)) {
            var task = (JavacTask) compiler.getTask(null, manager, diagnostics, List.of("-proc:none"), null,
                    manager.getJavaFileObjectsFromPaths(paths));
            var trees = Trees.instance(task);
            for (var unit: task.parse()) {
                String path = sourceRoot.relativize(Path.of(unit.getSourceFile().toUri())).toString();
                new TreeScanner<Void, Void>() {
                    private Set<String> configReporterParameters = Set.of();
                    private String currentClass = "";

                    @Override public Void visitClass(ClassTree declaration, Void unused) {
                        String previousClass = this.currentClass;
                        Set<String> previousParameters = this.configReporterParameters;
                        this.currentClass = previousClass.isEmpty()
                                ? String.valueOf(unit.getPackageName()) + "." + declaration.getSimpleName()
                                : previousClass + "." + declaration.getSimpleName();
                        this.configReporterParameters = Set.of();
                        try {
                            return super.visitClass(declaration, unused);
                        } finally {
                            this.currentClass = previousClass;
                            this.configReporterParameters = previousParameters;
                        }
                    }

                    @Override public Void visitMethod(MethodTree method, Void unused) {
                        Set<String> previous = this.configReporterParameters;
                        this.configReporterParameters = method.getParameters().stream()
                                .filter(parameter -> configReporterParameter(unit, parameter))
                                .map(parameter -> parameter.getName().toString()).collect(Collectors.toSet());
                        try {
                            return super.visitMethod(method, unused);
                        } finally {
                            this.configReporterParameters = previous;
                        }
                    }

                    private ObjectNode record(Tree site, String kind, String callee, String errorType, ExpressionTree expression) {
                        long offset = trees.getSourcePositions().getStartPosition(unit, site);
                        var entry = sites.addObject().put("source", path).put("line", unit.getLineMap().getLineNumber(offset))
                                .put("offset", offset).put("kind", kind).put("callee", callee)
                                .put("expression", expression.toString());
                        entry.put("error_type", errorType);
                        parts(expression, entry.putArray("parts"));
                        return entry;
                    }

                    /** These wrappers forward category and message unchanged to reportWarning.
                     * Match the declared receiver or owning class, not an arbitrary method name. */
                    private void recordConnectorWrapper(MethodInvocationTree call, String callee) {
                        var arguments = call.getArguments();
                        if (call.getMethodSelect() instanceof MemberSelectTree member
                                && member.getExpression() instanceof IdentifierTree receiver
                                && this.configReporterParameters.contains(receiver.getName().toString())) {
                            int category = callee.equals("warn") && arguments.size() == 2 ? 0
                                    : callee.equals("warnPath") && arguments.size() == 3 ? 1 : -1;
                            if (category >= 0)
                                this.record(call, "report", "ConfigReporter." + callee,
                                                literal(arguments.get(category)), arguments.get(category + 1))
                                        .put("wrapper_contract", CONFIG_REPORTER + "." + callee);
                        } else if (callee.equals("warnAt") && arguments.size() == 6
                                && this.currentClass.equals(CONNECTORS + ".ConnectorValidator")
                                && (call.getMethodSelect() instanceof IdentifierTree
                                || call.getMethodSelect() instanceof MemberSelectTree member
                                && member.getExpression().toString().equals("ConnectorValidator"))) {
                            this.record(call, "report", "ConnectorValidator.warnAt",
                                            literal(arguments.get(4)), arguments.get(5))
                                    .put("wrapper_contract", CONNECTORS + ".ConnectorValidator.warnAt");
                        }
                    }

                    @Override public Void visitMethodInvocation(MethodInvocationTree call, Void unused) {
                        String callee = name(call.getMethodSelect());
                        int count = call.getArguments().size();
                        if ((callee.equals("reportError") || callee.equals("reportWarning")) && (count == 3 || count == 4))
                            this.record(call, "report", callee, literal(call.getArguments().get(1)), call.getArguments().get(2));
                        else if (callee.equals("reportProblem") && count == 5)
                            this.record(call, "report", callee, literal(call.getArguments().get(3)), call.getArguments().get(4));
                        else
                            this.recordConnectorWrapper(call, callee);
                        return super.visitMethodInvocation(call, unused);
                    }

                    @Override public Void visitNewClass(NewClassTree constructor, Void unused) {
                        String callee = constructor.getIdentifier().toString();
                        var arguments = constructor.getArguments();
                        if (!arguments.isEmpty() && List.of("CompilationError", "UnsupportedException",
                                "UnimplementedException", "InternalCompilerError").contains(callee)) {
                            int message = callee.equals("CompilationError") && arguments.size() == 3 ? 2 : 0;
                            String errorType = switch (callee) {
                                case "CompilationError" -> message == 2 ? literal(arguments.get(1)) : "Compilation error";
                                case "UnsupportedException" -> "Not supported";
                                case "UnimplementedException" -> "Not yet implemented";
                                default -> null;
                            };
                            this.record(constructor, "constructor", callee, errorType, arguments.get(message));
                        }
                        return super.visitNewClass(constructor, unused);
                    }
                }.scan(unit, null);
            }
        }
        for (var diagnostic: diagnostics.getDiagnostics())
            if (diagnostic.getKind() == Diagnostic.Kind.ERROR)
                throw new IOException("Cannot inventory invalid Java source: " + diagnostic);
        return result;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IOException("Usage: DiagnosticTemplates JAVA_SOURCE_ROOT OUTPUT.json");
        var result = extract(Path.of(args[0]));
        Files.writeString(Path.of(args[1]), result.toPrettyString(), StandardOpenOption.CREATE_NEW);
    }
}

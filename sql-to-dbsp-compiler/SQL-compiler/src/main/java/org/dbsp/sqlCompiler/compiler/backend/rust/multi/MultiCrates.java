package org.dbsp.sqlCompiler.compiler.backend.rust.multi;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.circuit.ICircuit;
import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSimpleOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPViewDeclarationOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.BaseRustCodeGenerator;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustFileWriter;
import org.dbsp.sqlCompiler.compiler.backend.rust.RustWriter;
import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPPathExpression;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeComparator;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Data structure representing the crates generated for a program
 * when compiled using multiple crates. */
public class MultiCrates {
    final File rootDirectory;
    final CrateGenerator main;
    final CrateGenerator globals;
    final DBSPCompiler compiler;
    final List<CrateGenerator> operators;
    final Map<Integer, CrateGenerator> tupleCrates;
    final Map<Integer, CrateGenerator> semiCrates;
    @Nullable
    Map<String, DBSPDeclaration> declarationMap = null;

    final RustWriter.StructuresUsed used;
    final String pipelineName;
    public final static String FILE_PREFIX = "feldera_pipe_";

    public String getGlobalsName() {
        return FILE_PREFIX + this.pipelineName + "_globals";
    }

    public String getMainName() {
        return FILE_PREFIX + this.pipelineName + "_main";
    }

    public boolean enterprise() {
        return this.compiler.options.ioOptions.enterprise;
    }

    MultiCrates(File rootDirectory, String pipelineName, DBSPCompiler compiler, RustWriter.StructuresUsed used) {
        this.pipelineName = pipelineName;
        this.compiler = compiler;
        this.used = used;
        this.operators = new ArrayList<>();
        this.tupleCrates = new HashMap<>();
        this.semiCrates = new HashMap<>();
        this.rootDirectory = rootDirectory;
        boolean enterprise = this.enterprise();

        // One crate for each tuple size used
        for (int i : used.tupleSizesUsed) {
            if (used.isPredefined(i)) continue;
            RustWriter.StructuresUsed t = new RustWriter.StructuresUsed();
            t.tupleSizesUsed.add(i);
            BaseRustCodeGenerator tWriter = new RustFileWriter().setUsed(t).withUdf(false).withMalloc(false);
            CrateGenerator tuple = new CrateGenerator(
                    this.rootDirectory, FILE_PREFIX + "tuple" + i, tWriter, enterprise);
            Utilities.putNew(this.tupleCrates, i, tuple);
        }

        // One crate for each semigroup size used
        for (int i : used.semigroupSizesUsed) {
            RustWriter.StructuresUsed t = new RustWriter.StructuresUsed();
            t.semigroupSizesUsed.add(i);
            BaseRustCodeGenerator tWriter = new RustFileWriter().setUsed(t).withUdf(false).withMalloc(false);
            CrateGenerator semi = new CrateGenerator(
                    this.rootDirectory, FILE_PREFIX + "semi" + i, tWriter, enterprise);
            Utilities.putNew(this.semiCrates, i, semi);
        }

        CircuitWriter mainWriter = new CircuitWriter();
        BaseRustCodeGenerator globalsWriter = new RustFileWriter()
                .withUdf(true).withMalloc(false).withGenerateTuples(false);
        // Main crate contains the circuit
        this.main = new CrateGenerator(this.rootDirectory, this.getMainName(), mainWriter, enterprise);
        // Crate with global variables
        this.globals = new CrateGenerator(this.rootDirectory, this.getGlobalsName(), globalsWriter, enterprise);
    }

    CrateGenerator createOperatorCrate(DBSPCircuit circuit, DBSPOperator operator, ICircuit parent, boolean enterprise) {
        String name = FILE_PREFIX + operator.getNodeName(true);
        SingleOperatorWriter single = new SingleOperatorWriter(operator, circuit, parent);
        return new CrateGenerator(this.rootDirectory, name, single, enterprise);
    }

    static class UsesComparator extends InnerVisitor {
        public boolean found = false;

        public UsesComparator(DBSPCompiler compiler) {
            super(compiler);
        }

        public VisitDecision preorder(DBSPTypeComparator type) {
            this.found = true;
            return VisitDecision.STOP;
        }
    }

    static class UsesGlobals extends InnerVisitor {
        public boolean found = false;
        final Map<String, DBSPDeclaration> declarations;

        public UsesGlobals(DBSPCompiler compiler, Map<String, DBSPDeclaration> declarations) {
            super(compiler);
            this.declarations = declarations;
        }

        public VisitDecision preorder(DBSPPathExpression expression) {
            String string = expression.path.asString();
            if (this.declarations.containsKey(string)) {
                DBSPDeclaration decl = this.declarations.get(string);
                if (decl.item.getType().sameType(expression.getType()))
                    this.found = true;
            }
            return VisitDecision.STOP;
        }
    }

    boolean usesGlobals(DBSPOperator operator) {
        assert this.declarationMap != null;
        UsesGlobals finder = new UsesGlobals(this.compiler, this.declarationMap);
        CircuitVisitor visitor = finder.getCircuitVisitor(false);
        operator.accept(visitor);
        if (finder.found)
            return true;

        UsesComparator uc = new UsesComparator(this.compiler);
        for (var input: operator.inputs) {
            uc.apply(input.outputType());
        }
        return uc.found;
    }

    CrateGenerator tupleCrate(int tupleSize) {
        return Utilities.getExists(this.tupleCrates, tupleSize);
    }

    void addDependencies(CrateGenerator op, DBSPOperator operator) {
        this.main.addDependency(op);
        if (this.usesGlobals(operator))
            op.addDependency(this.globals);
        RustWriter.StructuresUsed locallyUsed = new RustWriter.StructuresUsed();
        RustWriter.FindResources finder = new RustWriter.FindResources(compiler, locallyUsed);
        CircuitVisitor circuitFinder = finder.getCircuitVisitor(false);
        if (!operator.is(DBSPNestedOperator.class))
            operator.accept(circuitFinder);

        for (var input : operator.inputs) {
            input.outputType().accept(finder);
        }
        for (int i = 0; i < operator.outputCount(); i++) {
            var out = operator.getOutput(i);
            out.outputType().accept(finder);
        }
        for (int i : locallyUsed.tupleSizesUsed) {
            if (locallyUsed.isPredefined(i)) continue;
            CrateGenerator gen = this.tupleCrate(i);
            op.addDependency(gen);
        }
        for (int i : locallyUsed.semigroupSizesUsed) {
            CrateGenerator gen = Utilities.getExists(this.semiCrates, i);
            op.addDependency(gen);
        }
    }

    void addNodes(List<IDBSPNode> nodes) {
        for (IDBSPNode node: nodes) {
            if (node.is(IDBSPInnerNode.class))
                this.globals.add(node);
            else {
                DBSPCircuit circuit = node.to(DBSPCircuit.class);
                this.main.add(node);
                // Add all declarations to the globals crate
                for (DBSPDeclaration decl: circuit.declarations)
                    this.globals.add(decl.item);
                this.declarationMap = circuit.declarationMap;
                for (DBSPOperator operator: circuit.allOperators) {
                    CrateGenerator op;
                    if (operator.is(DBSPNestedOperator.class)) {
                        DBSPNestedOperator nested = operator.to(DBSPNestedOperator.class);
                        NestedOperatorWriter writer = new NestedOperatorWriter(nested, circuit);
                        String name = FILE_PREFIX + operator.getNodeName(true);
                        op = new CrateGenerator(this.rootDirectory, name, writer, this.enterprise());
                        op.add(nested);
                        for (DBSPOperator inside: nested.getAllOperators()) {
                            if (inside.is(DBSPViewDeclarationOperator.class))
                                continue;
                            CrateGenerator insideOp = this.createOperatorCrate(
                                    circuit, inside.to(DBSPSimpleOperator.class), nested, this.enterprise());
                            this.addDependencies(insideOp, inside);
                            op.addDependency(insideOp);
                            this.operators.add(insideOp);
                        }
                    } else {
                        op = this.createOperatorCrate(circuit, operator, circuit, this.enterprise());
                    }

                    this.addDependencies(op, operator);
                    this.operators.add(op);
                }
            }
        }

        // Check to see whether the globals crate needs any tuples
        // and add them as dependencies.
        RustWriter.StructuresUsed used = this.globals.codeGenerator.to(RustFileWriter.class).analyze(this.compiler);
        for (var tupleSize: used.tupleSizesUsed) {
            if (used.isPredefined(tupleSize)) continue;
            CrateGenerator gen = this.tupleCrate(tupleSize);
            this.globals.addDependency(gen);
        }
    }

    void write() throws IOException {
        this.globals.write(this.compiler);
        File file = new File(new File(new File(globals.baseDirectory, globals.crateName), "src"),
                DBSPCompiler.UDF_FILE_NAME);
        if (!file.exists())
            Utilities.createEmptyFile(file.toPath());
        this.main.write(compiler);

        for (CrateGenerator gen: this.semiCrates.values())
            gen.write(this.compiler);
        for (CrateGenerator gen: this.tupleCrates.values())
            gen.write(this.compiler);
        Map<CrateGenerator, CrateGenerator> written = new HashMap<>();
        for (CrateGenerator op: this.operators) {
            if (written.containsKey(op)) {
                String current = op.dump(this.compiler);
                CrateGenerator prev = written.get(op);
                String previous = prev.dump(this.compiler);
                if (!current.equals(previous)) {
                    throw new InternalCompilerError("Hash collision for different crates\n" + current + "\n" + previous);
                }
            }
            written.put(op, op);
            op.write(this.compiler);
        }
    }
}

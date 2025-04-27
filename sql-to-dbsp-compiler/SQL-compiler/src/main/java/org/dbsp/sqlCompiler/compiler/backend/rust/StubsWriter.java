package org.dbsp.sqlCompiler.compiler.backend.rust;

import org.dbsp.sqlCompiler.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.circuit.DBSPDeclaration;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.DBSPFunction;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.statement.DBSPFunctionItem;
import org.dbsp.util.IndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Generates the stubs.rs file with declarations for the Rust user-defined functions */
public class StubsWriter extends BaseRustCodeGenerator {
    final Path path;
    final PrintStream stream;
    @Nullable
    DBSPCircuit circuit;

    public StubsWriter(Path path) throws IOException {
        this.stream = new PrintStream(Files.newOutputStream(path));
        this.setOutputBuilder(new IndentStream(this.stream));
        this.path = path;
        this.circuit = null;
    }

    // For a function prototype like f(s: i32) -> i32;
    // generate a body like
    // udf::f(s)
    DBSPFunction generateStubBody(DBSPFunction function) {
        String name = "udf::" + function.name;
        List<DBSPExpression> arguments = Linq.map(function.parameters, DBSPParameter::asVariable);
        DBSPExpression expression = new DBSPApplyExpression(name, function.returnType, arguments.toArray(new DBSPExpression[0]));
        return new DBSPFunction(
                function.getNode(), function.name, function.parameters,
                function.returnType, expression, function.annotations);
    }

    @Override
    public void add(IDBSPNode node) {
        this.circuit = node.to(DBSPCircuit.class);
    }

    @Override
    public void write(DBSPCompiler compiler) throws IOException {
        if (compiler.options.ioOptions.verbosity > 0)
            System.out.println("Writing UDF stubs to file " + this.path);
        this.builder().append("""
// Compiler-generated file.
// This file contains stubs for user-defined functions declared in the SQL program.
// Each stub defines a function prototype that must be implemented in `udf.rs`.
// Copy these stubs to `udf.rs`, replacing their bodies with the actual UDF implementation.
// See detailed documentation in https://docs.feldera.com/sql/udf.

#![allow(non_snake_case)]

use feldera_sqllib::*;
use crate::*;
""");
        List<DBSPFunction> extern = new ArrayList<>();
        Utilities.enforce(this.circuit != null);
        for (DBSPDeclaration decl: this.circuit.declarations) {
            DBSPFunctionItem item = decl.item.as(DBSPFunctionItem.class);
            if (item == null)
                continue;
            // Functions with a body do not need to be in stubs
            if (item.function.body != null)
                continue;
            extern.add(item.function);
        }

        for (DBSPFunction function : extern) {
            function = this.generateStubBody(function);
            String str = ToRustInnerVisitor.toRustString(compiler, function, false);
            this.builder().append(str).newline();
        }

        this.stream.close();
    }
}

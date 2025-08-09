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
import org.dbsp.util.IIndentStream;
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
    @Nullable
    PrintStream stream = null;
    @Nullable
    DBSPCircuit circuit;

    public StubsWriter(Path path) {
        this.path = path;
        this.circuit = null;
    }

    public IIndentStream builder() {
        try {
            // Allocate lazily; by that time the directory should be created.
            if (this.outputBuilder == null) {
                if (this.stream == null)
                    this.stream = new PrintStream(Files.newOutputStream(path));
                this.setOutputBuilder(new IndentStream(this.stream));
            }
            return this.outputBuilder;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
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
    public void write(DBSPCompiler compiler) {
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
        if (this.circuit != null) {
            for (DBSPDeclaration decl : this.circuit.declarations) {
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
                ToRustInnerVisitor.toRustString(compiler, this.builder(), function, null, false);
                this.builder().newline();
            }
        }
        Utilities.enforce(this.stream != null);
        this.stream.close();
    }
}

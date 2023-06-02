/*
 * Copyright 2023 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.backend.jit;

import org.dbsp.sqlCompiler.compiler.IErrorReporter;
import org.dbsp.sqlCompiler.compiler.backend.visitors.InnerRewriteVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.util.IModule;
import org.dbsp.util.Linq;

/**
 * Make sure every closure body is a BlockExpression.
 */
public class BlockClosures
        extends InnerRewriteVisitor
        implements IModule {

    public BlockClosures(IErrorReporter reporter) {
        super(reporter);
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        expression.body.accept(this);
        DBSPExpression newBody = this.getResultExpression();
        DBSPBlockExpression block ;
        if (!newBody.is(DBSPBlockExpression.class))
            block = new DBSPBlockExpression(Linq.list(), newBody);
        else
            block = newBody.to(DBSPBlockExpression.class);
        DBSPExpression result = expression;
        if (block != expression.body)
            result = new DBSPClosureExpression(expression.getNode(),
                block, expression.parameters);
        this.map(expression, result);
        return false;
    }
}

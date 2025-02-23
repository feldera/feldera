/*
 * Copyright 2022 VMware, Inc.
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

package org.dbsp.sqlCompiler.circuit.operator;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.compiler.ViewMetadata;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeIndexedZSet;
import org.dbsp.util.Utilities;

import java.util.List;

public final class DBSPSinkOperator extends DBSPViewBaseOperator {
    /** Create a SinkOperator.  May represent an output view or an index over a view.
     * TODO: this should probably be broken in two separate sublcasses for views and indexes.
     *
     * @param node             Calcite node represented.
     * @param viewName         Name of view or index represented by the sink.
     * @param queryOrViewName  Query defining the view, or the view name that is indexed.
     * @param originalRowType  Type of data of the view.  This is a struct type for a view,
     *                         and a raw tuple with 2 fields for an index: (key, value), each a struct.
     * @param metadata         Metadata of view (for an index this is the view's metadata).
     * @param input            Unique input stream. */
    public DBSPSinkOperator(CalciteObject node, ProgramIdentifier viewName, String queryOrViewName,
                            DBSPType originalRowType,
                            ViewMetadata metadata,
                            OutputPort input) {
        super(node, "inspect", null, viewName, queryOrViewName,
                originalRowType, metadata, input);
    }

    /** True if this sink corresponds to an INDEX statement */
    public boolean isIndex() {
        return this.outputType.is(DBSPTypeIndexedZSet.class);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public DBSPSimpleOperator withInputs(List<OutputPort> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPSinkOperator(
                    this.getNode(), this.viewName, this.query, this.originalRowType,
                    this.metadata, newInputs.get(0)).copyAnnotations(this);
        return this;
    }

    @SuppressWarnings("unused")
    public static DBSPSinkOperator fromJson(JsonNode node, JsonDecoder decoder) {
        ProgramIdentifier viewName = ProgramIdentifier.fromJson(Utilities.getProperty(node, "viewName"));
        String queryOrViewName = "";
        if (node.has("query"))
            queryOrViewName = Utilities.getStringProperty(node, "query");
        CommonInfo info = commonInfoFromJson(node, decoder);
        DBSPType originalRowType = fromJsonInner(node, "originalRowType", decoder, DBSPType.class);
        ViewMetadata metadata = ViewMetadata.fromJson(Utilities.getProperty(node, "metadata"), decoder);
        return new DBSPSinkOperator(CalciteObject.EMPTY, viewName, queryOrViewName,
                originalRowType, metadata, info.getInput(0))
                .addAnnotations(info.annotations(), DBSPSinkOperator.class);
    }
}

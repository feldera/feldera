package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;

/** Represents the DBSP ZWeight type as TypeUser. */
public final class DBSPTypeWeight extends DBSPTypeUser {
    public static final DBSPTypeWeight INSTANCE = new DBSPTypeWeight();

    private DBSPTypeWeight() {
        super(CalciteObject.EMPTY, DBSPTypeCode.USER, "Weight", false);
    }

    // sameType and hashCode inherited from TypeUser.

    @SuppressWarnings("unused")
    public static DBSPTypeWeight fromJson(JsonNode node, JsonDecoder decoder) {
        return INSTANCE;
    }
}

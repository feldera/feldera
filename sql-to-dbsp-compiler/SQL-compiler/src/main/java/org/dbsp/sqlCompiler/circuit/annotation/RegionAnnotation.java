package org.dbsp.sqlCompiler.circuit.annotation;

import org.dbsp.util.Utilities;

/** Interface implemented by annotations that are used to create DBSP circuit graph regions */
public abstract class RegionAnnotation extends Annotation {
    /** Get a tag which will be used to label the region */
    public abstract String getTag();
    /** A unique ID within the regions with the same tag */
    public abstract int getId();

    public String asVarName() {
        return this.getTag() + "_" + this.getId();
    }

    public String getTagQuoted() {
        return Utilities.doubleQuote(this.getTag(), true);
    }
}

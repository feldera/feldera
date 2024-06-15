package org.dbsp.simulator.util;

import javax.annotation.Nullable;

/** Utility interface providing some useful casting methods. */
public interface ICastable {
    @Nullable
    default <T> T as(Class<T> clazz) {
        return ICastable.as(this, clazz);
    }

    @Nullable
    static <T> T as(Object obj, Class<T> clazz) {
        if (clazz.isInstance(obj))
            return clazz.cast(obj);
        return null;
    }

    default <T> T to(Class<T> clazz) {
        T result = this.as(clazz);
        if (result == null)
            throw new RuntimeException(this + " is not an instance of " + clazz);
        return result;
    }

    default <T> boolean is(Class<T> clazz) {
        return clazz.isInstance(this);
    }
}

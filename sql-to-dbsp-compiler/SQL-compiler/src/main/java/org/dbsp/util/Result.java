package org.dbsp.util;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Class patterned after the Rust Result type, but with errors always a fixed type.
 *
 * @param data    Data represented by the result; can be null if the error is also null
 * @param error Error message
 */
public record Result<T>(@Nullable T data, @Nullable ErrorWithPosition error) {
    /**
     * Create a Result holding some data
     */
    public static <T> Result<T> ok(T ok) {
        return new Result<T>(ok, null);
    }

    /**
     * Create a result holding an error
     */
    public static <T> Result<T> err(ErrorWithPosition message) {
        return new Result<T>(null, message);
    }

    public boolean isOk() {
        return this.error == null;
    }

    public T ok() {
        return Objects.requireNonNull(this.data);
    }

    public ErrorWithPosition err() {
        return Objects.requireNonNull(this.error);
    }

    public boolean isErr() {
        return this.error != null;
    }
}

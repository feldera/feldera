package org.dbsp.simulator.util;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SuppressWarnings("UnusedReturnValue")
public interface IIndentStream {
    IIndentStream appendChar(char c);
    IIndentStream append(String string);
    IIndentStream append(boolean b);
    <T extends ToIndentableString> IIndentStream append(T value);
    IIndentStream append(int value);
    IIndentStream append(long value);
    /**
     * For lazy evaluation of the argument.
     */
    IIndentStream appendSupplier(Supplier<String> supplier);
    IIndentStream joinS(String separator, Collection<String> data);
    <T extends ToIndentableString> IIndentStream joinI(String separator, Collection<T> data);
    IIndentStream join(String separator, String[] data);
    IIndentStream join(String separator, Stream<String> data);
    <T> IIndentStream join(String separator, T[] data, Function<T, String> generator);
    <T> IIndentStream intercalate(String separator, T[] data, Function<T, String> generator);
    <T extends ToIndentableString> IIndentStream join(String separator, T[] data);
    IIndentStream join(String separator, Collection<String> data);
    IIndentStream intercalate(String separator, Collection<String> data);
    IIndentStream intercalate(String separator, String[] data);
    IIndentStream intercalateS(String separator, Collection<String> data);
    <T extends ToIndentableString> IIndentStream intercalateI(String separator, Collection<T> data);
    <T extends ToIndentableString> IIndentStream intercalateI(String separator, T[] data);
    IIndentStream newline();
    /**
     * Increase indentation and emit a newline.
     */
    IIndentStream increase();
    IIndentStream decrease();
}

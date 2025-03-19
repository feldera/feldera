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
 *
 *
 */

package org.dbsp.util;

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/** Some utility classes inspired by C# Linq. */
@SuppressWarnings("unused")
public class Linq {
    @SafeVarargs
    public static <T> Set<T> set(T... value) {
        return new HashSet<>(Linq.list(value));
    }

    static class MapIterator<T, S> implements Iterator<S> {
        final Iterator<T> data;
        final Function<T, S> map;

        MapIterator(Iterator<T> data, Function<T, S> function) {
            this.data = data;
            this.map = function;
        }

        @Override
        public boolean hasNext() {
            return this.data.hasNext();
        }

        public S next() {
            T next = this.data.next();
            return this.map.apply(next);
        }
    }

    static class MapIterable<T, S> implements Iterable<S> {
        final MapIterator<T, S> mapIterator;

        MapIterable(Iterable<T> data, Function<T, S> function) {
            this.mapIterator = new MapIterator<>(data.iterator(), function);
        }

        @Override
        public Iterator<S> iterator() {
            return this.mapIterator;
        }
    }

    public static <T, S> Iterable<S> map(Iterable<T> data, Function<T, S> function) {
        return new MapIterable<>(data, function);
    }

    public static <T, S> Iterator<S> map(Iterator<T> data, Function<T, S> function) {
        return new MapIterator<>(data, function);
    }

    public static <T, S> List<S> map(List<T> data, Function<T, S> function) {
        List<S> result = new ArrayList<>(data.size());
        for (T aData : data)
            result.add(function.apply(aData));
        return result;
    }

    public static <T> T[] concat(T[] array1, T[] array2) {
        T[] result = Arrays.copyOf(array1, array1.length + array2.length);
        System.arraycopy(array2, 0, result, array1.length, array2.length);
        return result;
    }

    public static <T, S> S[] map(T[] data, Function<T, S> function, Class<S> sc) {
        @SuppressWarnings("unchecked")
        S[] result = (S[])Array.newInstance(sc, data.length);
        for (int i = 0; i < data.length; i++)
            result[i] = function.apply(data[i]);
        return result;
    }

    public static @Nullable <T> T first(T[] data, Predicate<T> test) {
        for (T datum : data)
            if (test.test(datum))
                return datum;
        return null;
    }

    public static <T, S, R> R[] zip(T[] left, S[] right, BiFunction<T, S, R> function, Class<R> rc) {
        @SuppressWarnings("unchecked")
        R[] result = (R[])Array.newInstance(rc, Math.min(left.length, right.length));
        for (int i=0; i < result.length; i++)
            result[i] = function.apply(left[i], right[i]);
        return result;
    }

    public static <T, S, R> R[] zipSameLength(T[] left, S[] right, BiFunction<T, S, R> function, Class<R> rc) {
        if (left.length != right.length)
            throw new RuntimeException("Zipped arrays have different lengths " + left.length + " and " + right.length);
        @SuppressWarnings("unchecked")
        R[] result = (R[])Array.newInstance(rc, left.length);
        for (int i=0; i < result.length; i++)
            result[i] = function.apply(left[i], right[i]);
        return result;
    }

    public static <T> boolean same(@Nullable T[] left, @Nullable T[] right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        if (left.length != right.length)
            return false;
        for (int i = 0; i < left.length; i++)
            if (left[i] != right[i]) return false;
        return true;
    }

    public static List<Integer> range(int start, int exclusiveEnd) {
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = start; i < exclusiveEnd; i++)
            result.add(i);
        return result;
    }

    public static <T> List<T> append(List<T> list, T element) {
        ArrayList<T> result = new ArrayList<>(list);
        result.add(element);
        return result;
    }

    public static <T> boolean same(@Nullable Collection<T> left, @Nullable Collection<T> right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        if (left.size() != right.size())
            return false;
        Iterator<T> li = left.iterator();
        Iterator<T> ri = right.iterator();
        while (li.hasNext()) {
            T l = li.next();
            T r = ri.next();
            if (l != r)
                return false;
        }
        return true;
    }

    public static boolean sameStrings(List<String> left, List<String> right) {
        if (left.size() != right.size())
            return false;
        return Linq.all(Linq.zipSameLength(left, right, String::equals));
    }

    public static <T, S, R> List<R> zip(List<T> left, List<S> right, BiFunction<T, S, R> function) {
        int size = Math.min(left.size(), right.size());
        List<R> result = new ArrayList<>(size);
        for (int i=0; i < size; i++)
            result.add(function.apply(left.get(i), right.get(i)));
        return result;
    }

    public static <T, S, R> List<R> zipSameLength(Collection<T> left, Collection<S> right, BiFunction<T, S, R> function) {
        int size = left.size();
        if (size != right.size())
            throw new RuntimeException("Zipped lists have different lengths " + size + " and " + right.size());
        List<R> result = new ArrayList<>(size);
        Iterator<T> l = left.iterator();
        Iterator<S> r = right.iterator();
        for (int i=0; i < size; i++) {
            T t = l.next();
            S s = r.next();
            result.add(function.apply(t, s));
        }
        return result;
    }

    @SafeVarargs
    public static <T> List<T> list(T... data) {
        return new ArrayList<>(Arrays.asList(data));
    }

    public static <T> List<T> list(Iterable<T> data) {
        List<T> result = new ArrayList<>();
        data.forEach(result::add);
        return result;
    }

    public static <T> List<T> fill(int count, T value) {
        List<T> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            result.add(value);
        return result;
    }

    public static <T> List<T> list(Iterator<T> data) {
        List<T> result = new ArrayList<>();
        data.forEachRemaining(result::add);
        return result;
    }

    public static <T> List<T> where(Collection<T> data, Predicate<T> function) {
        List<T> result = new ArrayList<>();
        for (T aData : data)
            if (function.test(aData))
                result.add(aData);
        return result;
    }

    public static <T> T[] where(T[] data, Predicate<T> function, Class<T> tc) {
        List<T> result = new ArrayList<>();
        for (T datum : data)
            if (function.test(datum))
                result.add(datum);
        @SuppressWarnings("unchecked")
        T[] array = (T[])Array.newInstance(tc, result.size());
        return result.toArray(array);
    }

    public static boolean all(Iterable<Boolean> data) {
        for (Boolean b: data) {
            if (b == null || !b)
                return false;
        }
        return true;
    }

    public static boolean all(Boolean[] data) {
        for (Boolean b: data) {
            if (b == null || !b)
                return false;
        }
        return true;
    }

    public static <T> boolean any(Iterable<T> data, Predicate<T> test) {
        for (T d: data)
            if (test.test(d)) {
                return true;
            }
        return false;
    }

    public static boolean any(Iterable<Boolean> data) {
        for (Boolean d: data)
            if (d != null && d)
                return true;
        return false;
    }

    public static <T> boolean any(T[] data, Predicate<T> test) {
        for (T d: data)
            if (test.test(d))
                return true;
        return false;
    }

    public static <T> boolean all(Iterable<T> data, Predicate<T> test) {
        for (T d: data)
            if (!test.test(d))
                return false;
        return true;
    }

    public static <T> boolean all(T[] data, Predicate<T> test) {
        for (T d: data)
            if (!test.test(d))
                return false;
        return true;
    }

    public static <T> boolean different(@Nullable List<T> left, @Nullable List<T> right) {
        if (left == null)
            return right == null;
        if (right == null)
            return false;
        if (left.size() != right.size())
            return true;
        for (int i = 0; i < left.size(); i++)
            if (!left.get(i).equals(right.get(i)))
                return true;
        return false;
    }

    static public <T> boolean different(T[] left, T[] right) {
        if (left.length != right.length)
            return true;
        for (int i = 0; i < left.length; i++)
            if (!left[i].equals(right[i]))
                return true;
        return false;
    }

    static public <T> T reduce(T[] data, T zero, BiFunction<T, T, T> reducer) {
        T result = zero;
        for (T d: data) {
            result = reducer.apply(result, d);
        }
        return result;
    }
}

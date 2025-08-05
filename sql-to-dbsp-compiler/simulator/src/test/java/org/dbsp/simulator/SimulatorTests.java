package org.dbsp.simulator;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.math3.analysis.function.Add;
import org.dbsp.simulator.collections.IndexedZSet;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.IntegerSqlType;
import org.dbsp.simulator.types.IntegerWeightType;
import org.dbsp.simulator.types.SqlType;
import org.dbsp.simulator.types.StringSqlType;
import org.dbsp.simulator.types.TupleSqlType;
import org.dbsp.simulator.types.Weight;
import org.dbsp.simulator.values.BooleanSqlValue;
import org.dbsp.simulator.values.DynamicSqlValue;
import org.dbsp.simulator.values.IntegerSqlValue;
import org.dbsp.simulator.values.RuntimeFunction;
import org.dbsp.simulator.values.SqlTuple;
import org.dbsp.simulator.values.StringSqlValue;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class SimulatorTests {
    @Test
    public void zsetTests() {
        ZSet<SqlTuple> zero = new ZSet<>(IntegerWeightType.INSTANCE);
        Assert.assertEquals(0, zero.entryCount());
        ZSet<SqlTuple> some = new ZSet<>(IntegerWeightType.INSTANCE);
        SqlTuple tuple = new SqlTuple(new TupleSqlType(IntegerSqlType.INSTANCE, StringSqlType.INSTANCE))
                .add(new IntegerSqlValue(10))
                .add(new StringSqlValue("string"));

        some.append(tuple, IntegerWeightType.create(2));
        Assert.assertEquals("{\n    [10, 'string'] => 2\n}", some.toString());
        ZSet<SqlTuple> dbl = some.add(some);
        Assert.assertEquals("{\n    [10, 'string'] => 4\n}", dbl.toString());
        ZSet<SqlTuple> neg = dbl.negate();
        Assert.assertEquals("{\n    [10, 'string'] => -4\n}", neg.toString());
        ZSet<SqlTuple> z = dbl.add(neg);
        Assert.assertTrue(z.isEmpty());
        ZSet<SqlTuple> one = dbl.distinct();
        Assert.assertEquals("{\n    [10, 'string'] => 1\n}", one.toString());
        ZSet<SqlTuple> four = dbl.positive(false);
        Assert.assertEquals("{\n    [10, 'string'] => 4\n}", four.toString());
        ZSet<SqlTuple> none = neg.positive(false);
        Assert.assertTrue(none.isEmpty());
    }

    @JsonPropertyOrder({"name", "age"})
    public static class Person implements DynamicSqlValue {
        @Nullable
        public String name;
        public int age;

        @SuppressWarnings("unused")
        public Person() {}

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (age != person.age) return false;
            return Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + age;
            return result;
        }

        @Nullable
        public String getName() {
            return this.name;
        }

        @Override
        public SqlType getType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
            Person other = (Person) dynamicSqlValue;
            return Comparator.comparing(
                            Person::getName,
                            Comparator.nullsFirst(String::compareTo))
                    .thenComparing(x -> x.age,
                            Comparator.nullsFirst(Integer::compare))
                    .compare(this, other);
        }
    }

    @SuppressWarnings("CanBeFinal")
    public static class Age implements DynamicSqlValue {
        public int age;

        // Needed for deserializer
        @SuppressWarnings("unused")
        public Age() {
            this.age = 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Age age1 = (Age) o;

            return age == age1.age;
        }

        @Override
        public int hashCode() {
            return this.age;
        }

        @Override
        public String toString() {
            return "Age{" +
                    "age=" + age +
                    '}';
        }

        public Age(int age) {
            this.age = age;
        }

        @Override
        public SqlType getType() {
            return new TupleSqlType(IntegerSqlType.INSTANCE);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
            return Integer.compare(this.age, dynamicSqlValue.to(Age.class).age);
        }
    }

    public static <T extends DynamicSqlValue> ZSet<T> fromCSV(String data, Class<T> tclass) {
        try {
            CsvMapper mapper = new CsvMapper();
            CsvSchema schema = CsvSchema.emptySchema().withHeader();
            MappingIterator<T> it = mapper.readerFor(tclass)
                    .with(schema)
                    .readValues(data);
            List<T> collection = new ArrayList<>();
            it.readAll(collection);
            return new ZSet<>(collection, IntegerWeightType.INSTANCE);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T extends DynamicSqlValue> String toCsv(ZSet<T> data, Class<T> tclass) {
        try {
            Collection<T> values = data.toCollection();
            CsvMapper mapper = new CsvMapper();
            final CsvSchema schema = mapper.schemaFor(tclass).withUseHeader(true);
            ObjectWriter writer = mapper.writer(schema);
            StringWriter str = new StringWriter();
            writer.writeValue(str, values);
            return str.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ZSet<Person> getPersons() {
        String data = """
                name,age
                Billy,28
                Barbara,36
                John,12
                """;
        return fromCSV(data, Person.class);
    }

    @Test
    public void testSelect() {
        ZSet<Person> input = getPersons();
        ZSet<Age> ages = input.map(p -> new Age(p.age));
        ZSet<Age> expected = fromCSV("age\n28\n36\n12\n", Age.class);
        Assert.assertTrue(expected.equals(ages));

        ages = input.map(p -> new Age(p.age / 20));
        expected = fromCSV("age\n1\n1\n0\n", Age.class);
        Assert.assertTrue(expected.equals(ages));
    }

    @Test
    public void testWhere() {
        ZSet<Person> input = getPersons();
        ZSet<Person> adults = input.filter(
                new RuntimeFunction<>(
                        p -> new BooleanSqlValue(p.age >= 18)));
        ZSet<Person> expected = fromCSV("""
                name,age
                Billy,28
                Barbara,36
                """, Person.class);
        Assert.assertTrue(expected.equals(adults));
    }

    @SuppressWarnings("CanBeFinal")
    static final class AvgHelper {
        public int count;
        public int sum;

        AvgHelper(int count, int sum) {
            this.count = count;
            this.sum = sum;
        }
    }

    @Test
    public void testAggregate() {
        ZSet<Person> input = getPersons();

        AggregateDescription<Integer, Integer, Person> sum =
                new AggregateDescription<>(
                0, (a, p, w) -> a + p.age * w.asInteger(), r -> r);
        Integer ageSum = input.aggregate(sum);
        Assert.assertEquals(76, (int)ageSum);

        AggregateDescription<Integer, Integer, Person> count =
                new AggregateDescription<>(0, (a, p, w) -> a + w.asInteger(), r -> r);
        Integer personCount = input.aggregate(count);
        Assert.assertEquals(3, (int)personCount);

        AggregateDescription<Integer, Integer, Person> max =
                new AggregateDescription<>(
                        0, (a, p, w) -> Math.max(a, p.age), r -> r);
        Integer maxAge = input.aggregate(max);
        Assert.assertEquals(36, (int)maxAge);

        AggregateDescription<Integer, AvgHelper, Person> avg =
                new AggregateDescription<>(
                        new AvgHelper(0, 0),
                        (a, p, w) -> new AvgHelper(a.count + w.asInteger(), a.sum + p.age * w.asInteger()),
                        r -> r.sum / r.count);
        Integer avgAge = input.aggregate(avg);
        Assert.assertEquals(25, (int)avgAge);
    }

    @Test
    public void testExcept() {
        ZSet<Person> input = getPersons();
        ZSet<Person> adults = input.filter(new RuntimeFunction<>(
                null, p -> new BooleanSqlValue(p.age >= 18)));
        ZSet<Person> children = input.except(adults);
        ZSet<Person> expected = fromCSV("""
                name,age
                John,12
                """, Person.class);
        Assert.assertTrue(expected.equals(children));
    }

    @JsonPropertyOrder({"name", "city"})
    static class Address implements DynamicSqlValue {
        @Nullable
        public String name;
        @Nullable
        public String city;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Address address = (Address) o;

            if (!Objects.equals(name, address.name)) return false;
            return Objects.equals(city, address.city);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (city != null ? city.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Address{" +
                    "name='" + name + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }

        @Override
        public SqlType getType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Nullable
        public String getName() {
            return this.name;
        }

        @Override
        public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
            Address other = (Address) dynamicSqlValue;
            return Comparator.comparing(
                            Address::getName,
                            Comparator.nullsFirst(String::compareTo))
                    .thenComparing(x -> x.city,
                            Comparator.nullsFirst(String::compareTo))
                    .compare(this, other);
        }
    }

    ZSet<Address> getAddress() {
        return fromCSV("""
                name,city
                Billy,San Francisco
                Barbara,Seattle
                John,New York
                Tom,Miami
                """, Address.class);
    }

    @JsonPropertyOrder({"name", "address"})
    static class NameAddress implements DynamicSqlValue {
        @Nullable
        public String name;
        @Nullable
        public String address;

        @SuppressWarnings("unused")
        public NameAddress() {}

        public NameAddress(@Nullable String name, @Nullable String address) {
            this.name = name;
            this.address = address;
        }

        @Nullable
        public String getName() {
            return this.name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NameAddress that = (NameAddress) o;

            if (!Objects.equals(name, that.name)) return false;
            return Objects.equals(address, that.address);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (address != null ? address.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "NameAddress{" +
                    "name='" + name + '\'' +
                    ", address='" + address + '\'' +
                    '}';
        }

        @Override
        public SqlType getType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
            NameAddress other = (NameAddress) dynamicSqlValue;
            return Comparator.comparing(
                            NameAddress::getName,
                            Comparator.nullsFirst(String::compareTo))
                    .thenComparing(x -> x.address,
                            Comparator.nullsFirst(String::compareTo))
                    .compare(this, other);
        }
    }

    @Test
    public void testMultiply() {
        ZSet<Person> input = getPersons();
        ZSet<Address> address = getAddress();
        ZSet<NameAddress> product = input.multiply(address, (p, a) -> new NameAddress(p.name, a.city));
        Assert.assertEquals(input.entryCount() * address.entryCount(), product.entryCount());
    }

    @Test
    public void testIndex() {
        ZSet<Person> input = getPersons();
        IndexedZSet<StringSqlValue, Person> index = input.index(
                new RuntimeFunction<Person, StringSqlValue>(p -> new StringSqlValue(p.name != null ? p.name.substring(0, 1) : null)));
        Assert.assertEquals(2, index.groupCount());
    }

    @Test
    public void testUnion() {
        ZSet<Person> input = getPersons();
        ZSet<Person> adults = input.filter(new RuntimeFunction<>(p -> new BooleanSqlValue(p.age >= 18)));
        ZSet<Person> children = input.except(adults);
        ZSet<Person> all = adults.union(children);
        Assert.assertTrue(input.equals(all));
    }

    @Test
    public void testUnionAll() {
        ZSet<Person> input = getPersons();
        ZSet<Person> adults = input.filter(new RuntimeFunction<>(p -> new BooleanSqlValue(p.age >= 18)));
        ZSet<Person> all = input.union_all(adults);
        Weight johnCount = all.getWeight(new Person("John", 12));
        Assert.assertEquals(1, johnCount.asInteger());
        Weight billyCount = all.getWeight(new Person("Billy", 28));
        Assert.assertEquals(2, billyCount.asInteger());
        Weight tomCount = all.getWeight(new Person("Tom", 28));
        Assert.assertEquals(0, tomCount.asInteger());
    }

    @Test
    public void testGroupByAggregate() {
        ZSet<Person> input = getPersons();
        IndexedZSet<StringSqlValue, Person> index = input.index(
                new RuntimeFunction<>(p -> new StringSqlValue(p.name != null ? p.name.substring(0, 1) : null)));
        AggregateDescription<IntegerSqlValue, Integer, Person> max =
                new AggregateDescription<>(0, (a, p, w) -> Math.max(a, p.age), IntegerSqlValue::new);
        IndexedZSet<StringSqlValue, IntegerSqlValue> aggregate = index.aggregate(max);
        ZSet<Person> keyAge = aggregate.flatten((n, a) -> new Person(n.getValue(), a.getValue()));
        ZSet<Person> expected = fromCSV("name,age\nB,36\nJ,12\n", Person.class);
        Assert.assertTrue(expected.equals(keyAge));
    }

    @Test
    public void testDeindex() {
        ZSet<Person> input = getPersons();
        IndexedZSet<StringSqlValue, Person> personIndex = input.index(
                new RuntimeFunction<>(p -> new StringSqlValue(p.name)));
        ZSet<Person> flattened = personIndex.deindex();
        Assert.assertTrue(input.equals(flattened));
    }

    @Test
    public void testJoin() {
        ZSet<Person> input = getPersons();
        ZSet<Address> address = getAddress();
        IndexedZSet<StringSqlValue, Person> personIndex = input
                .filter(new RuntimeFunction<>(p -> new BooleanSqlValue(p.name != null)))
                .index(new RuntimeFunction<>(p -> new StringSqlValue(p.name)));
        IndexedZSet<StringSqlValue, Address> addressIndex = address
                .filter(new RuntimeFunction<>(a -> new BooleanSqlValue(a.name != null)))
                .index(new RuntimeFunction<>(a -> new StringSqlValue(a.name)));
        IndexedZSet<StringSqlValue, PersonAddressAge> join = personIndex.join(addressIndex, (p, a) -> new PersonAddressAge(p.name, p.age, a.city));
        ZSet<PersonAddressAge> result = join.deindex();
        ZSet<PersonAddressAge> expected = fromCSV("""
                name,age,city
                Barbara,36,Seattle
                Billy,28,"San Francisco"
                John,12,"New York\"""", PersonAddressAge.class);
        Assert.assertTrue(expected.equals(result));
    }

    @JsonPropertyOrder({"name", "age", "city"})
    public static class PersonAddressAge implements DynamicSqlValue {
        @Nullable public String name;
        public int age;
        @Nullable public String city;

        @SuppressWarnings("unused")
        public PersonAddressAge() {}

        @Nullable
        public String getName() {
            return this.name;
        }

        public PersonAddressAge(@Nullable String name, int age, @Nullable String city) {
            this.name = name;
            this.age = age;
            this.city = city;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PersonAddressAge that = (PersonAddressAge) o;

            if (age != that.age) return false;
            if (!Objects.equals(name, that.name)) return false;
            return Objects.equals(city, that.city);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + age;
            result = 31 * result + (city != null ? city.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "PersonAddressAge{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", city='" + city + '\'' +
                    '}';
        }

        @Override
        public SqlType getType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public int compareTo(@NotNull DynamicSqlValue dynamicSqlValue) {
            PersonAddressAge other = (PersonAddressAge) dynamicSqlValue;
            return Comparator.comparing(
                            PersonAddressAge::getName,
                            Comparator.nullsFirst(String::compareTo))
                    .thenComparing(x -> x.age,
                            Comparator.nullsFirst(Integer::compare))
                    .thenComparing(x -> x.city,
                            Comparator.nullsFirst(String::compareTo))
                    .compare(this, other);
        }
    }
}

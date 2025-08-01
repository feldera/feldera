package org.dbsp.simulator;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.dbsp.simulator.collections.IndexedZSet;
import org.dbsp.simulator.collections.ZSet;
import org.dbsp.simulator.types.IntegerWeightType;
import org.dbsp.simulator.types.StringSqlType;
import org.dbsp.simulator.values.IntegerSqlValue;
import org.dbsp.simulator.values.SqlTuple;
import org.dbsp.simulator.values.StringSqlValue;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class SimulatorTests {
    @Test
    public void zsetTests() {
        ZSet<SqlTuple, Integer> zero = new ZSet<>(IntegerWeightType.INSTANCE);
        Assert.assertEquals(0, zero.entryCount());
        ZSet<SqlTuple, Integer> some = new ZSet<>(IntegerWeightType.INSTANCE);
        SqlTuple tuple = new SqlTuple()
                .add(new IntegerSqlValue(10))
                .add(new StringSqlValue("string", new StringSqlType()));

        some.append(tuple, 2);
        Assert.assertEquals("{\n    [10, 'string'] => 2\n}", some.toString());
        ZSet<SqlTuple, Integer> dbl = some.add(some);
        Assert.assertEquals("{\n    [10, 'string'] => 4\n}", dbl.toString());
        ZSet<SqlTuple, Integer> neg = dbl.negate();
        Assert.assertEquals("{\n    [10, 'string'] => -4\n}", neg.toString());
        ZSet<SqlTuple, Integer> z = dbl.add(neg);
        Assert.assertTrue(z.isEmpty());
        ZSet<SqlTuple, Integer> one = dbl.distinct();
        Assert.assertEquals("{\n    [10, 'string'] => 1\n}", one.toString());
        ZSet<SqlTuple, Integer> four = dbl.positive(false);
        Assert.assertEquals("{\n    [10, 'string'] => 4\n}", four.toString());
        ZSet<SqlTuple, Integer> none = neg.positive(false);
        Assert.assertTrue(none.isEmpty());
    }

    @JsonPropertyOrder({"name", "age"})
    public static class Person {
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
    }

    @SuppressWarnings("CanBeFinal")
    public static class Age {
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
    }

    @SuppressWarnings("resource")
    public static <T> ZSet<T, Integer> fromCSV(String data, Class<T> tclass) {
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

    public <T> String toCsv(ZSet<T, Integer> data, Class<T> tclass) {
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

    public static ZSet<Person, Integer> getPersons() {
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
        ZSet<Person, Integer> input = getPersons();
        ZSet<Age, Integer> ages = input.map(p -> new Age(p.age));
        ZSet<Age, Integer> expected = fromCSV("age\n28\n36\n12\n", Age.class);
        Assert.assertTrue(expected.equals(ages));

        ages = input.map(p -> new Age(p.age / 20));
        expected = fromCSV("age\n1\n1\n0\n", Age.class);
        Assert.assertTrue(expected.equals(ages));
    }

    @Test
    public void testWhere() {
        ZSet<Person, Integer> input = getPersons();
        ZSet<Person, Integer> adults = input.filter(p -> p.age >= 18);
        ZSet<Person, Integer> expected = fromCSV("""
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
        ZSet<Person, Integer> input = getPersons();

        AggregateDescription<Integer, Integer, Person, Integer> sum =
                new AggregateDescription<>(
                0, (a, p, w) -> a + p.age * w, r -> r);
        Integer ageSum = input.aggregate(sum);
        Assert.assertEquals(76, (int)ageSum);

        AggregateDescription<Integer, Integer, Person, Integer> count =
                new AggregateDescription<>(0, (a, p, w) -> a + w, r -> r);
        Integer personCount = input.aggregate(count);
        Assert.assertEquals(3, (int)personCount);

        AggregateDescription<Integer, Integer, Person, Integer> max =
                new AggregateDescription<>(
                        0, (a, p, w) -> Math.max(a, p.age), r -> r);
        Integer maxAge = input.aggregate(max);
        Assert.assertEquals(36, (int)maxAge);

        AggregateDescription<Integer, AvgHelper, Person, Integer> avg =
                new AggregateDescription<>(
                        new AvgHelper(0, 0),
                        (a, p, w) -> new AvgHelper(a.count + w, a.sum + p.age * w),
                        r -> r.sum / r.count);
        Integer avgAge = input.aggregate(avg);
        Assert.assertEquals(25, (int)avgAge);
    }

    @Test
    public void testExcept() {
        ZSet<Person, Integer> input = getPersons();
        ZSet<Person, Integer> adults = input.filter(p -> p.age >= 18);
        ZSet<Person, Integer> children = input.except(adults);
        ZSet<Person, Integer> expected = fromCSV("""
                name,age
                John,12
                """, Person.class);
        Assert.assertTrue(expected.equals(children));
    }

    @JsonPropertyOrder({"name", "city"})
    static class Address {
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
    }

    ZSet<Address, Integer> getAddress() {
        return fromCSV("""
                name,city
                Billy,San Francisco
                Barbara,Seattle
                John,New York
                Tom,Miami
                """, Address.class);
    }

    @JsonPropertyOrder({"name", "address"})
    static class NameAddress {
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
    }

    @Test
    public void testMultiply() {
        ZSet<Person, Integer> input = getPersons();
        ZSet<Address, Integer> address = getAddress();
        ZSet<NameAddress, Integer> product = input.multiply(address, (p, a) -> new NameAddress(p.name, a.city));
        Assert.assertEquals(input.entryCount() * address.entryCount(), product.entryCount());
    }

    @Test
    public void testIndex() {
        ZSet<Person, Integer> input = getPersons();
        IndexedZSet<String, Person, Integer> index = input.index(p -> p.name != null ? p.name.substring(0, 1) : null);
        Assert.assertEquals(2, index.groupCount());
    }

    @Test
    public void testUnion() {
        ZSet<Person, Integer> input = getPersons();
        ZSet<Person, Integer> adults = input.filter(p -> p.age >= 18);
        ZSet<Person, Integer> children = input.except(adults);
        ZSet<Person, Integer> all = adults.union(children);
        Assert.assertTrue(input.equals(all));
    }

    @Test
    public void testUnionAll() {
        ZSet<Person, Integer> input = getPersons();
        ZSet<Person, Integer> adults = input.filter(p -> p.age >= 18);
        ZSet<Person, Integer> all = input.union_all(adults);
        Integer johnCount = all.getWeight(new Person("John", 12));
        Assert.assertEquals(1, (int)johnCount);
        Integer billyCount = all.getWeight(new Person("Billy", 28));
        Assert.assertEquals(2, (int)billyCount);
        Integer tomCount = all.getWeight(new Person("Tom", 28));
        Assert.assertEquals(0, (int)tomCount);
    }

    @Test
    public void testGroupByAggregate() {
        ZSet<Person, Integer> input = getPersons();
        IndexedZSet<String, Person, Integer> index = input.index(p -> p.name != null ? p.name.substring(0, 1) : null);
        AggregateDescription<Integer, Integer, Person, Integer> max =
                new AggregateDescription<>(0, (a, p, w) -> Math.max(a, p.age), r -> r);
        IndexedZSet<String, Integer, Integer> aggregate = index.aggregate(max);
        ZSet<Person, Integer> keyAge = aggregate.flatten(Person::new);
        ZSet<Person, Integer> expected = fromCSV("name,age\nB,36\nJ,12\n", Person.class);
        Assert.assertTrue(expected.equals(keyAge));
    }

    @Test
    public void testDeindex() {
        ZSet<Person, Integer> input = getPersons();
        IndexedZSet<String, Person, Integer> personIndex = input.index(p -> p.name);
        ZSet<Person, Integer> flattened = personIndex.deindex();
        Assert.assertTrue(input.equals(flattened));
    }

    @Test
    public void testJoin() {
        ZSet<Person, Integer> input = getPersons();
        ZSet<Address, Integer> address = getAddress();
        IndexedZSet<String, Person, Integer> personIndex = input.filter(p -> p.name != null).index(p -> p.name);
        IndexedZSet<String, Address, Integer> addressIndex = address.filter(a -> a.name != null).index(a -> a.name);
        IndexedZSet<String, PersonAddressAge, Integer> join = personIndex.join(addressIndex, (p, a) -> new PersonAddressAge(p.name, p.age, a.city));
        ZSet<PersonAddressAge, Integer> result = join.deindex();
        ZSet<PersonAddressAge, Integer> expected = fromCSV("""
                name,age,city
                Barbara,36,Seattle
                Billy,28,"San Francisco"
                John,12,"New York\"""", PersonAddressAge.class);
        Assert.assertTrue(expected.equals(result));
    }

    @JsonPropertyOrder({"name", "age", "city"})
    public static class PersonAddressAge {
        @Nullable public String name;
        public int age;
        @Nullable public String city;

        @SuppressWarnings("unused")
        public PersonAddressAge() {}

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
    }
}

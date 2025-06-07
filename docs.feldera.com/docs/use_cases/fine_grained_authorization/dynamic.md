# Implementing a dynamic FGA model

What if we wanted to allow not only the object graph, but also the access control
model itself to change at runtime? For instance, consider building a generic FGA platform
like OpenFGA, where users can define new object types, relationships, and rules.
With the approach we've taken so far, such changes would require creating a new SQL program
and re-computing all derived relationships from scratch. In a large system, this may cause
service disruptions, which may not be acceptable, especially if these changes occur frequently.

In this section we build an FGA implementation that supports incremental updates to the
authorization model in addition to incremental updates to the object graph.
The key idea is to model FGA rules as relational data instead of hard-coding them as SQL queries.

The implementation described below is available as a [pre-packaged use case
in the Feldera online sandbox](https://try.feldera.com/create/?name=dynamic-fga) as well
as in your local Feldera installation.

:::note

The implementation in this section prioritizes simplicity and clarity over performance.
While it is not the most optimized solution, it is intentionally designed to effectively illustrate
the concept.

:::

## Modeling objects and relationships

In the interests of clarity, we use strings instead of integers for unique object IDs, allowing
objects to be referenced by their names.

```sql
create type id_t as string;
```

We start by modeling all object types as a single table. This approach allows new object types
to be introduced without requiring additional tables. Since different object types have varying
sets of attributes, we use a dynamically-typed representation of attributes with the
[`VARIANT` type](/sql/json). Think of a `VARIANT` instance as a JSON document.

```sql
create type properties_t as variant;

-- All objects in the object graph.
create table objects (
    id id_t not null primary key,
    properties properties_t
);
```

Next, we model all relationships that form the edges of the object graph as a separate table.
Following common FGA terminology, a relationship connects a **subject** (the entity holding a
permission) to a **resource** (the entity to which the relationship grants access):

```sql
create table relationships (
    -- Subject id (reference to the `objects` table).
    subject_id id_t not null,
    -- Resource id (reference to the `objects` table).
    resource_id id_t not null,
    relationship id_t not null
);
```

Here are some example relationships:

| subject_id  | resource_id  | relationship |
|-------------|--------------|--------------|
| emily       | engineering  | member       |
| accounting  | financials   | editor       |
| folder1     | folder2      | parent       |

## Modeling rules as data

We need a way to model logical conditions as part of FGA rules. A condition
is a Boolean function over subject and resource attributes. Since SQL does not support
function-typed values, we represent conditions as strings containing expressions,
e.g., `subject.is_banned != true`.
We use [JMESPath](https://jmespath.org/) syntax for expressions.  JMESPath is a JSON query
language similar to JSONPath, but with better support for evaluating conditions over JSON
documents (rather than just filtering and extracting values from the document):

```sql
-- JMESPath expression.
create type predicate_t as string;
```

The JMESPath expression that defines the condition has access to two predefined variables:
`subject` and `resource`, which store subject and resource attributes respectively,
e.g., ``subject.is_banned != `true` ``.

We create a [user-defined function (UDF)](/sql/udf) that evaluates
JMESPath expressions:

```sql
-- Returns `true` if the expression evaluates to `true`, `false` if it evaluates to any other value
-- and `NULL` if `condition` is not a valid JMESPAth expression.
create function check_condition(
    condition predicate_t,
    subject_properties properties_t,
    resource_properties properties_t
) returns boolean;
```

The implementation of this UDF in Rust is given below. It uses the [`jmespath` crate](https://crates.io/crates/jmespath)
to evaluate the JMESPath expression:

<details>
<summary> `check_condition` UDF implementation in Rust </summary>

```rust
use std::collections::BTreeMap;
use feldera_sqllib::Variant;

pub fn check_condition(
    condition: Option<SqlString>,
    subject_properties: Option<Variant>,
    resource_properties: Option<Variant>)
-> Result<Option<bool>, Box<dyn std::error::Error>> {
    Ok(do_check_condition(condition, subject_properties, resource_properties))
}

pub fn do_check_condition(
    condition: Option<SqlString>,
    subject_properties: Option<Variant>,
    resource_properties: Option<Variant>)
-> Option<bool> {
    let condition = condition?;
    let subject_properties = subject_properties?;
    let resource_properties = resource_properties?;

    let expr = jmespath::compile(condition.str()).map_err(|e| println!("invalid jmes expression: {e}")).ok()?;
    let all_properties = Variant::Map(BTreeMap::from(
        [(Variant::String(SqlString::from_ref("subject")), subject_properties),
         (Variant::String(SqlString::from_ref("resource")), resource_properties)]));

    let result = expr.search(all_properties).map_err(|e| println!("error evaluating jmes expression: {e}")).ok()?;
    Some(result.as_ref() == &jmespath::Variable::Bool(true))
}
```
</details>

We can now define tables that store FGA rules. As before we model rules with one and two
prerequisites only:

```sql
-- Rules with one pre-requisite:
--
-- prerequisite_relationship(object1, object2) and condition(object1, object2) -> derived_relationship(object1, object2)
create table unary_rules (
    prerequisite_relationship id_t,
    condition predicate_t,
    derived_relationship id_t
);

-- Rules with two pre-requisites.
--
-- prerequisite1_relationship(object1, object2) and prerequisite2_relationship(object2, object3) and condition(object1, object3)
--     -> derived_relationship(object1, object3)
create table binary_rules (
    prerequisite1_relationship id_t,
    prerequisite2_relationship id_t,
    condition predicate_t,
    derived_relationship id_t
);
```

As an example, here is a complete set of rules for the file management service:

Unary rules:
| prerequisite_relationship | condition | derived_relationship |
|---------------------------|-----------|----------------------|
| editor                    | `true`    | group-can-write      |
| viewer                    | `true`    | group-can-read       |
| group-can-write           | `true`    | group-can-read       |

Binary rules:
| prerequisite1_relationship | prerequisite2_relationship | condition                       | derived_relationship |
|----------------------------|----------------------------|---------------------------------|----------------------|
| group-can-write            | parent                     | `true`                          | group-can-write      |
| group-can-read             | parent                     | `true`                          | group-can-read       |
| member                     | group-can-write            | subject.is_banned != `true`     | user-can-write       |
| member                     | group-can-read             | subject.is_banned != `true`     | user-can-read        |

The final step is to write SQL views that evaluate these rules over the object graph.

```sql
-- Relationships derived using unary rules.
declare recursive view derived_unary_relationships (
    subject_id id_t not null,
    resource_id id_t not null,
    relationship id_t
);

-- Relationships derived using binary rules.
declare recursive view derived_binary_relationships (
    subject_id id_t not null,
    resource_id id_t not null,
    relationship id_t
);

-- All derived relationships.
declare recursive view derived_relationships (
    subject_id id_t not null,
    resource_id id_t not null,
    relationship id_t
);

create materialized view derived_unary_relationships as
select
    derived_relationships.subject_id,
    derived_relationships.resource_id,
    unary_rules.derived_relationship as relationship
from
    derived_relationships
    join unary_rules on derived_relationships.relationship = unary_rules.prerequisite_relationship
    join objects subject on subject.id = derived_relationships.subject_id
    join objects resource on resource.id = derived_relationships.resource_id
where
    check_condition(unary_rules.condition, subject.properties, resource.properties);

create materialized view derived_binary_relationships as
select
    r1.subject_id,
    r2.resource_id,
    binary_rules.derived_relationship as relationship
from
derived_relationships r1
    join binary_rules on r1.relationship = binary_rules.prerequisite1_relationship
    join derived_relationships r2 on r1.resource_id = r2.subject_id and binary_rules.prerequisite2_relationship = r2.relationship
    join objects subject on subject.id = r1.subject_id
    join objects resource on resource.id = r2.resource_id
where
    check_condition(binary_rules.condition, subject.properties, resource.properties);

create materialized view derived_relationships as
select * from relationships
union all
select * from derived_unary_relationships
union all
select * from derived_binary_relationships;
```

### Lights, Camera, Action!

Let's see if it works. Open the complete code provided above in one of the following environments:
* Feldera online sandbox: [https://try.feldera.com/create/?name=dynamic-fga](https://try.feldera.com/create/?name=dynamic-fga) OR
* Your local Feldera instance: [127.0.0.1:8080/create/?name=dynamic-fga](http://127.0.0.1:8080/create/?name=dynamic-fga)

Start the pipeline and create rules for the file manager example using ad hoc queries:

```sql
insert into unary_rules values
  ('editor', '`true`', 'group-can-write'),         -- Rule 1.
  ('viewer', '`true`', 'group-can-read'),          -- Rule 3.
  ('group-can-write', '`true`', 'group-can-read'); -- Rule 4.

insert into binary_rules values
  ('group-can-write', 'parent', '`true`', 'group-can-write'),                     -- Rule 2.
  ('group-can-read', 'parent', '`true`', 'group-can-read'),                       -- Rule 5.
  ('member', 'group-can-write', 'subject.is_banned != `true`', 'user-can-write'), -- Rule 6.
  ('member', 'group-can-read', 'subject.is_banned != `true`', 'user-can-read');   -- Rule 7.
```

Populate the object graph:

```sql
insert into objects values
  ('user:emily', '{"is_banned": false}'),
  ('user:irene', '{"is_banned": false}'),
  ('user:adam', '{"is_banned": true}'),
  ('group:engineering', '{}'),
  ('group:it', '{}'),
  ('group:accounting', '{}'),
  ('file:designs', '{}'),
  ('file:financials', '{}'),
  ('file:f1', '{}'),
  ('file:f2', '{}'),
  ('file:f3', '{}');

insert into relationships values
  ('user:emily', 'group:engineering', 'member'),
  ('user:irene', 'group:it', 'member'),
  ('user:adam', 'group:accounting', 'member'),
  ('group:engineering', 'file:designs', 'editor'),
  ('group:it', 'file:designs', 'editor'),
  ('group:it', 'file:financials', 'editor'),
  ('group:accounting', 'file:financials', 'editor'),
  ('group:accounting', 'file:designs', 'reader'),
  ('file:designs', 'file:f1', 'parent'),
  ('file:designs', 'file:f2', 'parent'),
  ('file:financials', 'file:f3', 'parent');
```

Validate the output of the program:

```sql
select *
from derived_relationships
where
  relationship = 'user-can-read';
```

| subject_id   | resource_id     | relationship    |
|--------------|-----------------|-----------------|
| user:emily   | file:designs    | user-can-read   |
| user:irene   | file:financials | user-can-read   |
| user:irene   | file:f3         | user-can-read   |
| user:emily   | file:f1         | user-can-read   |
| user:irene   | file:f1         | user-can-read   |
| user:irene   | file:f2         | user-can-read   |
| user:irene   | file:designs    | user-can-read   |
| user:emily   | file:f2         | user-can-read   |

## Changing the rules

With this new implementation, we gain a level of flexibility that was not achievable with our initial static FGA
implementation. Specifically, we can now dynamically add, remove, and modify rules, relationships, and object types as needed.
For example, consider enhancing our file manager authorization model by introducing an owner relationship. In this model, if
a group is designated as the `owner` of a file, all users within that group will have the permission to permanently delete
the file from the system.

The corresponding FGA rules are:
* **Rule 8:** `owner(group, file) -> group-can-write(group, file)`.
* **Rule 9:** `owner(group, file) -> group-can-permanently-delete(group, file)`.
* **Rule 10:** `group-can-permanently-delete(group, file1) and parent(file1, file2) -> group-can-permanently-delete(group, file2)`.
* **Rule 11:** `member(user, group) and group-can-permanently-delete(group, file) and (not user.is_banned) -> user-can-permanently-delete(user, file)`.

In SQL:

```sql
insert into unary_rules values
  ('owner', '`true`', 'group-can-write'),
  ('owner', '`true`', 'group-can-permanently-delete');

insert into binary_rules values
  ('group-can-permanently-delete', 'parent', '`true`', 'group-can-permanently-delete'),
  ('member', 'group-can-permanently-delete', 'subject.is_banned != `true`', 'user-can-permanently-delete');
```

Let's see if it worked.  Make `group:engineering` an owner of `file:designs` and list all derived
`user-can-permanently-delete` relationships:

```sql
insert into relationships values ('group:engineering', 'file:designs', 'owner');

select *
from derived_relationships
where
  relationship = 'user-can-permanently-delete';
```

| subject_id  | resource_id  | relationship                 |
|-------------|--------------|------------------------------|
| user:emily  | file:designs | user-can-permanently-delete  |
| user:emily  | file:f1      | user-can-permanently-delete  |
| user:emily  | file:f2      | user-can-permanently-delete  |

With only a few lines of code, we  have built a fully dynamic FGA implementation that can incrementally reconfigure
itself on the fly with 0 downtime or service disruption!

## Optimizations

The main shortcoming of proactively computing all relationships that can be derived from the object graph is that the
number of all possible derived relationships can be very large, making them expensive to compute and store; however only
a very small subset of these relationships will typically be accessed at runtime. We therefore want to optimize our
computation to only derive the subset of relationships that can be accessed by authorization checks performed by the system.
For example, only users who are logged into the system can issue authorization requests; hence we only need to consider
currently active users in the computation.  When a new user logs in, their permissions can be incrementally computed on the fly.

Similarly, not all resources in the system can be addressable at any given time, for example it's possible that users can
only access files when they are browsing their parent folder.  In this case, we should only track permissions for currently
active resources.  Let's implement this last optimization as an example.  Replace the `create table objects(..);` declaration
with the following fragment:

```sql
-- All objects in the system, including active and currently inactive objects.
create table all_objects (
    id id_t not null primary key,
    properties properties_t
);

-- Currently active objects.
--
-- This table is a subset of object ids, including only those objects for which authorization rules need to be evaluated.
-- The definition of an "active object" varies depending on the application and may include, for example, folders or wiki pages
-- currently accessed or open by at least one user.
create table active_objects(
    object_id id_t not null
);

-- Relevant objects are all active objects plus all objects from which
-- an active object can be reached by following object graph edges.
declare recursive view relevant_objects(
    object_id id_t not null
);

create view relevant_objects as
select * from active_objects
union all
select relationships.subject_id
    from relevant_objects join relationships on relevant_objects.object_id = relationships.resource_id;

-- Objects whose id's are in `relevant_objects`.
create materialized view objects as
select all_objects.*
    from all_objects join relevant_objects on all_objects.id = relevant_objects.object_id;
```

The new program will only compute derived relationships over objects in `relevant_objects` only.

## Takeaways

Whether you are adding a flexible authorization layer to your application or building a general-purpose policy
framework, Feldera offers a high-performance off-the-shelf compute engine for the job. Two features of Feldera
make it ideal for this purpose:

1. **Incremental query evaluation** enables Feldera to handle changes to the object graph or the FGA model
 in real-time by avoiding complete recomputation.
2. **Support for mutually recursive views** allows naturally capturing iterative graph traversal with recursive queries.

With these capabilities, Feldera stands out as a powerful solution for advanced authorization and policy management tasks.

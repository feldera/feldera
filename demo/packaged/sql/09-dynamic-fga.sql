-- Use Case: A Dynamic Policy Engine (dynamic-fga)
--
-- A policy engine implementation that supports incremental updates to access control rules.
--

/*
This program implements a Fine-Grained Authorization (FGA) policy engine that supports incremental
updates to the authorization model in addition to incremental updates to the object graph.
It is part of the Fine-Grained Authorization tutorial: https://docs.feldera.com/use_cases/fine_grained_authorization
Follow the tutorial for a detailed explanation.
*/

-- We use strings as unique ids, so we can refer to various entities by name.
create type id_t as string;

-- We model properties of an object as a JSON map.
create type properties_t as variant;

-- A predicate is a condition over a subject and a resource that must hold for a
-- rule to fire. Predicates are written as JMESPath queries over a JSON object
-- with two fields `subject` and `resource` that contain subject and resource
-- properties respectively:
--
-- {
--    subject: {..},
--    resource: {..}
-- }
create type predicate_t as string;

-- Returns `true` if the expression evaluates to `true`, `false` if it evaluates to any other value
-- and `NULL` if `condition` is not a valid JMESPAth expression.
create function check_condition(
    condition predicate_t,
    subject_properties properties_t,
    resource_properties properties_t
) returns boolean;

-- All entities in the system, including both users and resources.
create table objects (
    id id_t not null primary key,
    properties properties_t
);

-- Edges in the authorization graph that represent basic facts about the system specified by the user.
--
-- An edge specifies a subject, a resource, and a relation betweent them, e.g.,
-- (folder1, folder2, parent) or (user1, group1, member).
create table relationships (
    -- Subject id (reference to the `objects` table).
    subject_id id_t not null,
    -- Resource id (reference to the `objects` table).
    resource_id id_t not null,
    relationship id_t not null
);

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
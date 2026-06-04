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

SET FELDERA_IGNORE_WARNING_UNUSED = 1;

-- We use strings as unique ids, so we can refer to various entities by name.
CREATE TYPE id_t AS STRING;

-- We model properties of an object as a JSON map.
CREATE TYPE properties_t AS VARIANT;

-- A predicate is a condition over a subject and a resource that must hold for a
-- rule to fire. Predicates are written as JMESPath queries over a JSON object
-- with two fields `subject` and `resource` that contain subject and resource
-- properties respectively:
--
-- {
--    subject: {..},
--    resource: {..}
-- }
CREATE TYPE predicate_t AS STRING;

-- Returns `true` if the expression evaluates to `true`, `false` if it evaluates to any other value
-- and `NULL` if `condition` is not a valid JMESPAth expression.
CREATE FUNCTION check_condition(
    condition predicate_t,
    subject_properties properties_t,
    resource_properties properties_t
) RETURNS BOOLEAN;

-- All entities in the system, including both users and resources.
CREATE TABLE objects (
    id id_t NOT NULL PRIMARY KEY,
    properties properties_t
);

-- Edges in the authorization graph that represent basic facts about the system specified by the user.
--
-- An edge specifies a subject, a resource, and a relation betweent them, e.g.,
-- (folder1, folder2, parent) or (user1, group1, member).
CREATE TABLE relationships (
    -- Subject id (reference to the `objects` table).
    subject_id id_t NOT NULL,
    -- Resource id (reference to the `objects` table).
    resource_id id_t NOT NULL,
    relationship id_t NOT NULL
);

-- Rules with one pre-requisite:
--
-- prerequisite_relationship(object1, object2) and condition(object1, object2) -> derived_relationship(object1, object2)
CREATE TABLE unary_rules (
    prerequisite_relationship id_t,
    condition predicate_t,
    derived_relationship id_t
);

-- Rules with two pre-requisites.
--
-- prerequisite1_relationship(object1, object2) and prerequisite2_relationship(object2, object3) and condition(object1, object3)
--     -> derived_relationship(object1, object3)
CREATE TABLE binary_rules (
    prerequisite1_relationship id_t,
    prerequisite2_relationship id_t,
    condition predicate_t,
    derived_relationship id_t
);

-- Relationships derived using unary rules.
DECLARE RECURSIVE VIEW derived_unary_relationships (
    subject_id id_t NOT NULL,
    resource_id id_t NOT NULL,
    relationship id_t
);

-- Relationships derived using binary rules.
DECLARE RECURSIVE VIEW derived_binary_relationships (
    subject_id id_t NOT NULL,
    resource_id id_t NOT NULL,
    relationship id_t
);

-- All derived relationships.
DECLARE RECURSIVE VIEW derived_relationships (
    subject_id id_t NOT NULL,
    resource_id id_t NOT NULL,
    relationship id_t
);

CREATE MATERIALIZED VIEW derived_unary_relationships AS
SELECT
    derived_relationships.subject_id,
    derived_relationships.resource_id,
    unary_rules.derived_relationship AS relationship
FROM
    derived_relationships
    JOIN unary_rules ON derived_relationships.relationship = unary_rules.prerequisite_relationship
    JOIN objects subject ON subject.id = derived_relationships.subject_id
    JOIN objects resource ON resource.id = derived_relationships.resource_id
WHERE
    check_condition(unary_rules.condition, subject.properties, resource.properties);

CREATE MATERIALIZED VIEW derived_binary_relationships AS
SELECT
    r1.subject_id,
    r2.resource_id,
    binary_rules.derived_relationship AS relationship
FROM
derived_relationships r1
    JOIN binary_rules ON r1.relationship = binary_rules.prerequisite1_relationship
    JOIN derived_relationships r2 ON r1.resource_id = r2.subject_id AND binary_rules.prerequisite2_relationship = r2.relationship
    JOIN objects subject ON subject.id = r1.subject_id
    JOIN objects resource ON resource.id = r2.resource_id
WHERE
    check_condition(binary_rules.condition, subject.properties, resource.properties);

CREATE MATERIALIZED VIEW derived_relationships AS
SELECT * FROM relationships
UNION ALL
SELECT * FROM derived_unary_relationships
UNION ALL
SELECT * FROM derived_binary_relationships;

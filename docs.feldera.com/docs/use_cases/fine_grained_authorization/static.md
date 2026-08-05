# Implementing a static FGA model

In this section we will implement the file manager authorization model using Feldera.
We model objects, relationships, and rules described above as SQL tables and views. We
will see how FGA rules naturally lend themselves to implementation as SQL queries.
We will also see how Feldera can evaluate these queries efficiently and
incrementally at scale.

The approach we take here works best when the authorization model is co-designed with
the application and doesn't change at runtime: the application can create and destroy
users, groups, and files, but not change the access control rules.

The implementation described in this section is available as a [pre-packaged example
in the Feldera online sandbox](https://try.feldera.com/create/?name=fine-grained-auth) as well
as in your local Feldera installation.

## Modeling the object graph

We start with modeling the three object types—users, groups, and files—as SQL tables:

```sql
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING,
    is_banned BOOL
) WITH ('materialized' = 'true');

CREATE TABLE groups (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING
) WITH ('materialized' = 'true');

CREATE TABLE files (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING,
    -- Parent folder id; NULL for a root folder.
    parent_id BIGINT
) WITH ('materialized' = 'true');
```

Note that the `parent_id` field models the `parent` relationship between files.

Next, we model `member`, `editor`, and `viewer` relationships:

```sql
-- Member relationship models user membership in groups.
CREATE TABLE members (
    id BIGINT NOT NULL PRIMARY KEY,
    user_id  BIGINT NOT NULL,
    group_id BIGINT NOT NULL
) WITH ('materialized' = 'true');

-- Editor relationship between a group and a file that gives the group the permission
-- to read or write the file.
CREATE TABLE group_file_editor (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
) WITH ('materialized' = 'true');

-- Viewer relationship between a group and a file that gives the group the permission to read the file.
CREATE TABLE group_file_viewer (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
) WITH ('materialized' = 'true');
```

## Implementing rules

So far, we've defined objects and relationships that comprise the object graph of our application.
We are now ready to implement derived relationships.  We start with the `group-can-write`
relationship defined by the following two rules.

* **Rule 1:** `editor(group, file) -> group-can-write(group, file)`.
* **Rule 2:** `group-can-write(group, file1) and parent(file1, file2) -> group-can-write(group, file2)`.

In Rule 2 `group-can-write` appears on both sides of the implication, indicating that this is a
recursive relationship. Rule 1 specifies the base case: a group has write access to all files
for which it is an editor. Rule 2 defines the recursive step: write permissions propagate from a
file to all its children.  We implement these rules as a recursive SQL view:

```sql
DECLARE RECURSIVE VIEW group_can_write (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
);

CREATE MATERIALIZED VIEW group_can_write AS
-- Rule 1: editor(group, file) -> group-can-write(group, file).
(
  SELECT group_id, file_id FROM group_file_editor
)
UNION ALL
-- Rule 2: group-can-write(group, file1) and parent(file1, file2) -> group-can-write(group, file2).
(
  SELECT
    group_can_write.group_id,
    files.id AS file_id
  FROM
    group_can_write JOIN files ON group_can_write.file_id = files.parent_id
);
```

Rules for the `group-can-read` relationship have a similar structure, with one additional
rule (Rule 4), which states that the write permission to a file (`group-can-write`) implies
the read permission `group-can-read`:

* **Rule 3:** `viewer(group, file) -> group-can-read(group, file)`.
* **Rule 4:** `group-can-write(group, file) -> group-can-read(group, file)`.
* **Rule 5:** `group-can-read(group, file1) and parent(file1, file2) -> group-can-read(group, file2)`.

```sql
DECLARE RECURSIVE VIEW group_can_read (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
);

CREATE MATERIALIZED VIEW group_can_read AS
-- Rule 3: viewer(group, file) -> group-can-read(group, file).
(
  SELECT group_id, file_id FROM group_file_viewer
)
UNION ALL
-- Rule 4: group-can-write(group, file) -> group-can-read(group, file).
(
  SELECT group_id, file_id FROM group_can_write
)
UNION ALL
-- Rule 5: group-can-read(group, file1) and parent(file1, file2) -> group-can-read(group, file2).
(
  SELECT
    group_can_read.group_id,
    files.id AS file_id
 FROM
    group_can_read JOIN files ON group_can_read.file_id = files.parent_id
);
```

Finally, we implement `user-can-write` and `user-can-read` relationships:

* **Rule 6:** `member(user, group) and group-can-write(group, file) and (not user.is_banned) -> user-can-write(user, file)`.
* **Rule 7:** `member(user, group) and group-can-read(group, file) and (not user.is_banned) -> user-can-read(user, file)`.

```sql
-- Rule 6: member(user, group) and group-can-write(group, file) and (not user.is_banned) -> user-can-write(user, file).
CREATE MATERIALIZED VIEW user_can_write AS
SELECT DISTINCT
    members.user_id,
    group_can_write.file_id
FROM
    members
    JOIN group_can_write ON members.group_id = group_can_write.group_id
    JOIN users ON users.id = members.user_id
WHERE NOT users.is_banned;

-- Rule 7: member(user, group) and group-can-read(group, file) and (not user.is_banned) -> user-can-read(user, file).
CREATE MATERIALIZED VIEW user_can_read AS
SELECT DISTINCT
    members.user_id,
    group_can_read.file_id
FROM
    members
    JOIN group_can_read ON members.group_id = group_can_read.group_id
    JOIN users ON users.id = members.user_id
WHERE NOT users.is_banned;
```

This is it! With a few lines of SQL we implemented an incremental recursive FGA engine.

## Kicking the tires

Copy the complete SQL code below to the Feldera Web Console (or click `Run` to open it in the Feldera
online sandbox).

<details>
<summary> Expand to see full SQL code </summary>

```sql
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING,
    is_banned BOOL
) WITH ('materialized' = 'true');

CREATE TABLE groups (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING
) WITH ('materialized' = 'true');

CREATE TABLE files (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING,
    -- Parent folder id when not NULL
    parent_id BIGINT
) WITH ('materialized' = 'true');

-- Member relationship models user membership in groups.
CREATE TABLE members (
    id BIGINT NOT NULL PRIMARY KEY,
    user_id  BIGINT NOT NULL,
    group_id BIGINT NOT NULL
) WITH ('materialized' = 'true');

-- Editor relationship between a group and a file that gives the group the permission
-- to read or write the file.
CREATE TABLE group_file_editor (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
) WITH ('materialized' = 'true');

-- Viewer relationship between a group and a file that gives the group the permission to read the file.
CREATE TABLE group_file_viewer (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
) WITH ('materialized' = 'true');

DECLARE RECURSIVE VIEW group_can_write (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
);

CREATE MATERIALIZED VIEW group_can_write AS
-- Rule 1: editor(group, file) -> group-can-write(group, file).
(
  SELECT group_id, file_id FROM group_file_editor
)
UNION ALL
-- Rule 2: group-can-write(group, file1) and parent(file1, file2) -> group-can-write(group, file2).
(
  SELECT
    group_can_write.group_id,
    files.id AS file_id
  FROM
    group_can_write JOIN files ON group_can_write.file_id = files.parent_id
);

DECLARE RECURSIVE VIEW group_can_read (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
);

CREATE MATERIALIZED VIEW group_can_read AS
-- Rule 3: viewer(group, file) -> group-can-read(group, file).
(
  SELECT group_id, file_id FROM group_file_viewer
)
UNION ALL
-- Rule 4: group-can-write(group, file) -> group-can-read(group, file).
(
  SELECT group_id, file_id FROM group_can_write
)
UNION ALL
-- Rule 5: group-can-read(group, file1) and parent(file1, file2) -> group-can-read(group, file2).
(
  SELECT
    group_can_read.group_id,
    files.id AS file_id
 FROM
    group_can_read JOIN files ON group_can_read.file_id = files.parent_id
);

-- Rule 6: member(user, group) and group-can-write(group, file) and (not user.is_banned) -> user-can-write(user, file).
CREATE MATERIALIZED VIEW user_can_write AS
SELECT DISTINCT
    members.user_id,
    group_can_write.file_id
FROM
    members
    JOIN group_can_write ON members.group_id = group_can_write.group_id
    JOIN users ON users.id = members.user_id
WHERE NOT users.is_banned;

-- Rule 7: member(user, group) and group-can-read(group, file) and (not user.is_banned) -> user-can-read(user, file).
CREATE MATERIALIZED VIEW user_can_read AS
SELECT DISTINCT
    members.user_id,
    group_can_read.file_id
FROM
    members
    JOIN group_can_read ON members.group_id = group_can_read.group_id
    JOIN users ON users.id = members.user_id
WHERE NOT users.is_banned;
```
</details>

Start the pipeline and populate the object graph to match the [example](intro.md#object-graph) by issuing the following
ad hoc queries:

```sql
INSERT INTO users VALUES
    (1, 'emily', FALSE),
    (2, 'irene', FALSE),
    (3, 'adam', TRUE);

INSERT INTO groups VALUES
    (1, 'engineering'),
    (2, 'it'),
    (3, 'accounting');

INSERT INTO files VALUES
    (1, 'designs', NULL),
    (2, 'financials', NULL),
    (3, 'f1', 1),
    (4, 'f2', 1),
    (5, 'f3', 2);

INSERT INTO members VALUES
    (1, 1, 1), -- emily is in engineering
    (2, 2, 2), -- irene is in IT
    (3, 3, 3); -- adam is in accounting

INSERT INTO group_file_editor VALUES
    (1, 1),         -- 'engineering' can edit 'designs'
    (2, 1), (2, 2), -- 'it' can edit 'designs' and 'financials'
    (3, 2);         -- 'accounting' can edit 'financials'.

INSERT INTO group_file_viewer VALUES
    (3, 1); -- 'accounting' can view 'designs'.
```

We can now validate the output of the program, e.g.:

```sql
SELECT
  users.name AS user_name,
  files.name AS file_name
FROM
  user_can_read
  JOIN users ON users.id = user_can_read.user_id
  JOIN files ON files.id = user_can_read.file_id;
```

| user_name | file_name   |
|-----------|-------------|
| emily     | designs     |
| irene     | designs     |
| irene     | financials  |
| irene     | f3          |
| emily     | f1          |
| irene     | f1          |
| emily     | f2          |
| irene     | f2          |

As expected, `emily`, being a member of `engineering`, has read access to all files under the `designs` folder,
while `irene`, a member of `it`, can read files under both `designs` and `financials`.

Next we make an incremental change to the object graph, adding `emily` to the `it` group:

```sql
INSERT INTO members VALUES (4, 1, 2);
```

Running the `select` query above will return two _additional_ rows:

| user_name | file_name   |
|-----------|-------------|
| emily     | financials  |
| emily     | f3          |


## Running at full speed

The SQL code below demonstrates the same program as before, but now configured
with a data generator that builds a random object graph with 1,000 users, 100 groups,
100 top-level folders, 1,000 sub-folders, and 100,000 files randomly distributed
across the sub-folders. The generator runs continuously, dynamically updating the
random set of 100,000 files. Additionally, it continuously modifies user group
memberships.

<details>
<summary> Expand to see full SQL code </summary>

```sql
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING,
    is_banned BOOL
) WITH (
  'materialized' = 'true',
  -- Generate 1000 random users
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 1000,
            "fields": {
                "name": { "strategy": "name" }
            }
        }]
      }
    }
  }]'
);

CREATE TABLE groups (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING
) WITH (
  'materialized' = 'true',
  -- Generate 100 random groups
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 100,
            "fields": {
                "name": { "strategy": "word" }
            }
        }]
      }
    }
  }]'
);

CREATE TABLE files (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING,
    -- Parent folder id when not NULL
    parent_id BIGINT
) WITH (
  'materialized' = 'true',
  -- Generate a file hierarchy with 100 top-level folders, 1,000 sub-folders, and 100,000 files
  -- randomly distributed across sub-folders. The generator will continue running indefinitely
  -- randomly updating the 100,000 files.
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 100,
            "fields": {
                "id": { "range": [0, 100] },
                "name": { "strategy": "word" },
                "parent_id": { "null_percentage": 100 }
            }
        },
        {
            "limit": 1000,
            "fields": {
                "id": { "range": [100, 1100] },
                "name": { "strategy": "word" },
                "parent_id": { "range": [0,100] }
            }
        },
        {
            "fields": {
                "id": { "range": [1100, 101100] },
                "name": { "strategy": "word" },
                "parent_id": { "range": [100,1100], "strategy": "uniform" }
            }
        }
        ]
      }
    }
  }]'
);

-- Member relationship models user membership in groups.
CREATE TABLE members (
    id BIGINT NOT NULL PRIMARY KEY,
    user_id  BIGINT NOT NULL,
    group_id BIGINT NOT NULL
) WITH (
  'materialized' = 'true',
  -- Assign each use to 3 randomly selected groups. The generator will continue running indefinitely
  -- randomly re-assigning users to groups.
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "rate": 1000,
            "fields": {
                "id": { "range": [0,3000] },
                "user_id": {"range": [0, 1000], "strategy": "zipf"},
                "group_id": {"range": [0, 100], "strategy": "zipf"}
            }
        }]
      }
    }
  }]'
);

-- Editor relationship between a group and a file that gives the group the permission
-- to read or write the file.
CREATE TABLE group_file_editor (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
) WITH (
  'materialized' = 'true',
  -- Randomly assign one group as an editor to each top-level folder.
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 100,
            "fields": {
                "group_id": {"range": [0, 100], "strategy": "uniform"},
                "file_id": {"range": [0, 100] }
            }
        }]
      }
    }
  }]'
);

-- Viewer relationship between a group and a file that gives the group the permission to read the file.
CREATE TABLE group_file_viewer (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
) WITH (
  'materialized' = 'true',
  -- Give viewer permissions to 10 randomly selected subfolders to each user group.
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
            "limit": 1000,
            "fields": {
                "group_id": {"range": [0, 100]},
                "file_id": {"range": [100, 1100], "strategy": "uniform" }
            }
        }]
      }
    }
  }]'
);

DECLARE RECURSIVE VIEW group_can_write (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
);

CREATE MATERIALIZED VIEW group_can_write AS
-- Rule 1: editor(group, file) -> group-can-write(group, file).
(
  SELECT group_id, file_id FROM group_file_editor
)
UNION ALL
-- Rule 2: group-can-write(group, file1) and parent(file1, file2) -> group-can-write(group, file2).
(
  SELECT
    group_can_write.group_id,
    files.id AS file_id
  FROM
    group_can_write JOIN files ON group_can_write.file_id = files.parent_id
);

DECLARE RECURSIVE VIEW group_can_read (
    group_id BIGINT NOT NULL,
    file_id BIGINT NOT NULL
);

CREATE MATERIALIZED VIEW group_can_read AS
-- Rule 3: viewer(group, file) -> group-can-read(group, file).
(
  SELECT group_id, file_id FROM group_file_viewer
)
UNION ALL
-- Rule 4: group-can-write(group, file) -> group-can-read(group, file).
(
  SELECT group_id, file_id FROM group_can_write
)
UNION ALL
-- Rule 5: group-can-read(group, file1) and parent(file1, file2) -> group-can-read(group, file2).
(
  SELECT
    group_can_read.group_id,
    files.id AS file_id
 FROM
    group_can_read JOIN files ON group_can_read.file_id = files.parent_id
);

-- Rule 6: member(user, group) and group-can-write(group, file) and (not user.is_banned) -> user-can-write(user, file).
CREATE MATERIALIZED VIEW user_can_write AS
SELECT DISTINCT
    members.user_id,
    group_can_write.file_id
FROM
    members
    JOIN group_can_write ON members.group_id = group_can_write.group_id
    JOIN users ON users.id = members.user_id
WHERE NOT users.is_banned;

-- Rule 7: member(user, group) and group-can-read(group, file) and (not user.is_banned) -> user-can-read(user, file).
CREATE MATERIALIZED VIEW user_can_read AS
SELECT DISTINCT
    members.user_id,
    group_can_read.file_id
FROM
    members
    JOIN group_can_read ON members.group_id = group_can_read.group_id
    JOIN users ON users.id = members.user_id
WHERE NOT users.is_banned;
```
</details>

Running on a MacBook Pro with M3 Max CPU, this program achieves sustained throughput of 115K updates/s,
meaning that it processes 115K object graph changes/s and updates all derived relationships.

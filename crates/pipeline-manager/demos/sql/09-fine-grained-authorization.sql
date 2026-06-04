-- Example: Fine-Grained Authorization (fine-grained-auth)
--
-- Implement a policy engine using mutually recursive SQL queries.
--

/*
This example demonstrates how to build a high-performance, real-time policy engine using Feldera.
It is part of the Fine-Grained Authorization tutorial: https://docs.feldera.com/use_cases/fine_grained_authorization

We implement a simple access control model for a shared file management service. The model operates
with three types of objects: (1) users, (2) groups, and (3) files.

Objects are connected by the following relationships:
- `member` - a relationship between a user and a group.
- `parent` - a relationship between files that defines the folder hierarchy.
- `editor` - a relationship between a group and a file that gives the group the permission to read or write the file.
- `viewer` - a relationship between a group and a file that gives the group the permission to read the file.

We define four derived relationships:
- `group-can-read(group, file)` - `group` is allowed to read `file`.
- `group-can-write(group, file)` - `group` is allowed to write `file`.
- `user-can-read(user, file)` - `user` is allowed to read `file`.
- `user-can-write(user, file)` - `group` is allowed to write `file`.

These relationships are governed by the following rules:
- Rule 1: `editor(group, file) -> group-can-write(group, file)` - if a group is an editor of a file, it can write this file.
- Rule 2: `group-can-write(group, file1) and parent(file1, file2) -> group-can-write(group, file2)` - if a group can write
  a file, then it can write any of its children.
- Rule 3: `viewer(group, file) -> group-can-read(group, file)` - if a group is a viewer of a file, then it can read this file.
- Rule 4: `group-can-write(group, file) -> group-can-read(group, file)` - the write permission to a file implies the read
  permission to the same file.
- Rule 5: `group-can-read(group, file1) and parent(file1, file2) -> group-can-read(group, file2)` - if a group can read a file,
  then it can read any of its children.
- Rule 6: `member(user, group) and group-can-write(group, file) and (not user.is_banned) -> user-can-write(user, file)` - if a user is a member
  of a group that can write a file and the user is not banned, then the user can write the file.
- Rule 7: `member(user, group) and group-can-read(group, file) and (not user.is_banned) -> user-can-read(user, file)` - if a user is a member
  of a group that can read a file and the user is not banned, then the user can read the file.

The SQL program below incrementally evaluates these rules over dynamically changing object graph.
*/

SET FELDERA_IGNORE_WARNING_UNUSED_COLUMN = 1;
SET FELDERA_IGNORE_WARNING_UNUSED = 1;

CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    name STRING,
    is_banned BOOL
) WITH (
  'materialized' = 'true',
  -- Generate 1000 random users
  'connectors' = '[{
    "name": "users",
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
    "name": "groups",
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
    "name": "files",
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
            "rate": 1000,
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
    "name": "members",
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
    "name": "group_file_editor",
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
    "name": "group_file_viewer",
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

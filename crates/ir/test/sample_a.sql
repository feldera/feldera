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

create table users (
    id bigint not null primary key,
    name string,
    is_banned bool
) with (
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

create table groups (
    id bigint not null primary key,
    name string
) with (
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

create table files (
    id bigint not null primary key,
    name string,
    -- Parent folder id when not NULL
    parent_id bigint
) with (
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
create table members (
    id bigint not null primary key,
    user_id  bigint not null,
    group_id bigint not null
) with (
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
create table group_file_editor (
    group_id bigint not null,
    file_id bigint not null
) with (
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
create table group_file_viewer (
    group_id bigint not null,
    file_id bigint not null
) with (
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

declare recursive view group_can_write (
    group_id bigint not null,
    file_id bigint not null
);

create materialized view group_can_write as
-- Rule 1: editor(group, file) -> group-can-write(group, file).
(
  select group_id, file_id from group_file_editor
)
union all
-- Rule 2: group-can-write(group, file1) and parent(file1, file2) -> group-can-write(group, file2).
(
  select
    group_can_write.group_id,
    files.id as file_id
  from
    group_can_write join files on group_can_write.file_id = files.parent_id
);

declare recursive view group_can_read (
    group_id bigint not null,
    file_id bigint not null
);

create materialized view group_can_read as
-- Rule 3: viewer(group, file) -> group-can-read(group, file).
(
  select group_id, file_id from group_file_viewer
)
union all
-- Rule 4: group-can-write(group, file) -> group-can-read(group, file).
(
  select group_id, file_id from group_can_write
)
union all
-- Rule 5: group-can-read(group, file1) and parent(file1, file2) -> group-can-read(group, file2).
(
  select
    group_can_read.group_id,
    files.id as file_id
 from
    group_can_read join files on group_can_read.file_id = files.parent_id
);

-- Rule 6: member(user, group) and group-can-write(group, file) and (not user.is_banned) -> user-can-write(user, file).
create materialized view user_can_write as
select distinct
    members.user_id,
    group_can_write.file_id
from
    members
    join group_can_write on members.group_id = group_can_write.group_id
    join users on users.id = members.user_id
where not users.is_banned;

-- Rule 7: member(user, group) and group-can-read(group, file) and (not user.is_banned) -> user-can-read(user, file).
create materialized view user_can_read as
select distinct
    members.user_id,
    group_can_read.file_id
from
    members
    join group_can_read on members.group_id = group_can_read.group_id
    join users on users.id = members.user_id
where not users.is_banned;

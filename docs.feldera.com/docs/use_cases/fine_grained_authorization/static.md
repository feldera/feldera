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
create table users (
    id bigint not null primary key,
    name string,
    is_banned bool
) with ('materialized' = 'true');

create table groups (
    id bigint not null primary key,
    name string
) with ('materialized' = 'true');

create table files (
    id bigint not null primary key,
    name string,
    -- Parent folder id; NULL for a root folder.
    parent_id bigint
) with ('materialized' = 'true');
```

Note that the `parent_id` field models the `parent` relationship between files.

Next, we model `member`, `editor`, and `viewer` relationships:

```sql
-- Member relationship models user membership in groups.
create table members (
    id bigint not null primary key,
    user_id  bigint not null,
    group_id bigint not null
) with ('materialized' = 'true');

-- Editor relationship between a group and a file that gives the group the permission
-- to read or write the file.
create table group_file_editor (
    group_id bigint not null,
    file_id bigint not null
) with ('materialized' = 'true');

-- Viewer relationship between a group and a file that gives the group the permission to read the file.
create table group_file_viewer (
    group_id bigint not null,
    file_id bigint not null
) with ('materialized' = 'true');
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
```

Rules for the `group-can-read` relationship have a similar structure, with one additional
rule (Rule 4), which states that the write permission to a file (`group-can-write`) implies
the read permission `group-can-read`:

* **Rule 3:** `viewer(group, file) -> group-can-read(group, file)`.
* **Rule 4:** `group-can-write(group, file) -> group-can-read(group, file)`.
* **Rule 5:** `group-can-read(group, file1) and parent(file1, file2) -> group-can-read(group, file2)`.

```sql
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
```

Finally, we implement `user-can-write` and `user-can-read` relationships:

* **Rule 6:** `member(user, group) and group-can-write(group, file) and (not user.is_banned) -> user-can-write(user, file)`.
* **Rule 7:** `member(user, group) and group-can-read(group, file) and (not user.is_banned) -> user-can-read(user, file)`.

```sql
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
```

This is it! With a few lines of SQL we implemented an incremental recursive FGA engine.

## Kicking the tires

Copy the complete SQL code below to the Feldera Web Console (or click `Run` to open it in the Feldera
online sandbox).

<details>
<summary> Expand to see full SQL code </summary>

```sql
create table users (
    id bigint not null primary key,
    name string,
    is_banned bool
) with ('materialized' = 'true');

create table groups (
    id bigint not null primary key,
    name string
) with ('materialized' = 'true');

create table files (
    id bigint not null primary key,
    name string,
    -- Parent folder id when not NULL
    parent_id bigint
) with ('materialized' = 'true');

-- Member relationship models user membership in groups.
create table members (
    id bigint not null primary key,
    user_id  bigint not null,
    group_id bigint not null
) with ('materialized' = 'true');

-- Editor relationship between a group and a file that gives the group the permission
-- to read or write the file.
create table group_file_editor (
    group_id bigint not null,
    file_id bigint not null
) with ('materialized' = 'true');

-- Viewer relationship between a group and a file that gives the group the permission to read the file.
create table group_file_viewer (
    group_id bigint not null,
    file_id bigint not null
) with ('materialized' = 'true');

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
```
</details>

Start the pipeline and populate the object graph to match the [example](intro#object-graph) by issuing the following
ad hoc queries:

```sql
insert into users values
    (1, 'emily', false),
    (2, 'irene', false),
    (3, 'adam', true);

insert into groups values
    (1, 'engineering'),
    (2, 'it'),
    (3, 'accounting');

insert into files values
    (1, 'designs', NULL),
    (2, 'financials', NULL),
    (3, 'f1', 1),
    (4, 'f2', 1),
    (5, 'f3', 2);

insert into members values
    (1, 1, 1), -- emily is in engineering
    (2, 2, 2), -- irene is in IT
    (3, 3, 3); -- adam is in accounting

insert into group_file_editor values
    (1, 1),         -- 'engineering' can edit 'designs'
    (2, 1), (2, 2), -- 'it' can edit 'designs' and 'financials'
    (3, 2);         -- 'accounting' can edit 'financials'.

insert into group_file_viewer values
    (3, 1); -- 'accounting' can view 'designs'.
```

We can now validate the output of the program, e.g.:

```sql
select
  users.name as user_name,
  files.name as file_name
from
  user_can_read
  join users on users.id = user_can_read.user_id
  join files on files.id = user_can_read.file_id;
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
insert into members values (4, 1, 2);
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
```
</details>

Running on a MacBook Pro with M3 Max CPU, this program achieves sustained throughput of 115K updates/s,
meaning that it processes 115K object graph changes/s and updates all derived relationships.

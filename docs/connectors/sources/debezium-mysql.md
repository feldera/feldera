# Debezium-MySQL input connector

Feldera can consume a stream of changes to a MySQL database through the use of
[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
with the [Debezium](https://debezium.io/) plug-in installed.
This connection to Feldera can be done in four steps:

1. **Enable binary logging (binlog):** configure the MySQL database to enable the
   binlog with row-based format. The row-based binary log describes changes
   (e.g., row inserts, updates and deletes) that can then be ingested by
   Kafka Connect with the Debezium plug-in.

2. **Create user for access:** within your MySQL database, create a dedicated user
   for the Debezium access.

3. **Create the Kafka Connect input connector:** contact the Kafka Connect instance
   to create a Kafka Connect input connector which is connected to the MySQL database.
   In turn, Kafka Connect will create Kafka topics automatically based on the database
   schema it detects. Each Kafka topic corresponds to a table.

4. **Create Feldera input connectors:** create a Feldera input connector to the
   Kafka topic with Debezium-MySQL formatting. For each table, a Feldera input
   connector must be created.
   

The Debezium parts of this article are based on the [**official Debezium tutorial**](https://debezium.io/documentation/reference/tutorial.html),
which provides more detailed information and notes regarding Debezium connectors.


## Step 1: Enable binlog

Binary logging with row-based format must be enabled.


### Generic: MySQL

MySQL version 8.0 has binary logging enabled by default.
As such, no settings have to be adjusted unless your database
is configured differently than the default.
* Debezium has [some recommendations regarding MySQL setup](https://debezium.io/documentation/reference/stable/connectors/mysql.html#setting-up-mysql).
* Along with an [example configuration](https://github.com/debezium/container-images/blob/main/examples/mysql/2.3/mysql.cnf).


### AWS Aurora

Aurora is a MySQL database offered by AWS. By default, binlog is not enabled.
It is possible to enable the binlog via the AWS Console.
[**Please follow the instructions that AWS provides.**](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_LogAccess.MySQL.BinaryFormat.html)
Note that for Debezium, the binlog format must be set to ROW.


## Step 2: Debezium user

Within your database, you must create a user which has the privileges
to access the binlog. Note that the binlog is global, and as such contains
**all row data** across **all tables** across **all databases**.

1. Below is the SQL to create the dedicated Debezium user with the privileges.
   **Be sure to fill in your own secure and unique username and password.**

   ```sql
   CREATE USER 'chosen_debezium_username' IDENTIFIED BY 'chosen_debezium_password';
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'debezium';
   ```

2. We will refer to the chosen username/password as the Debezium username/password.


## Step 3: Create Kafka Connect input connector

Within your Feldera Cloud, you must have already set up a Kafka and
Kafka Connect (with the Debezium plug-in) instance.
We will now connect the MySQL database with Kafka Connect.

1. Collect the following information:

    * `[KAFKA CONNECT HOSTNAME:PORT]` : The Kafka Connect hostname and port separated by a colon.
    * `[KAFKA HOSTNAME:PORT]` : The Kafka broker hostname and port separated by a colon.
    * `[MYSQL HOST NAME]` : The hostname of your MySQL instance.
    * `[MYSQL PORT]` : The port of your MySQL instance (generally 3306)
    * `[DEBEZIUM USERNAME]` : The created user's username from Step 2 (above)
    * `[DEBEZIUM PASSWORD]` : The created user's password from Step 2 (above)
    * `[UNIQUE DATABASE SERVER ID]` : Unique identifier of your database server. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/replication-options.html) for more information.
    * `[UNIQUE DATABASE SERVER NAME]` : Unique name of your database server which will be prefixed to all Kafka topics.
    * `[DATABASES TO CONNECT]` : List of databases (within your MySQL database) to connect

2. Create the Debezium-MySQL connector using an HTTP POST request to the
   Kafka Connect instance. This is achieved via a `curl` command in which you fill
   in the information above.

   **NOTE: the curl command below is only for demo purposes.
   Passwords or other secrets should not be put in plaintext in connector configurations.
   They should be managed/externalized with appropriate secret management.
   For more information, see the notes in the
   [official Debezium tutorial](https://debezium.io/documentation/reference/tutorial.html)
   as well as [KIP-297](https://cwiki.apache.org/confluence/display/KAFKA/KIP-297%3A+Externalizing+Secrets+for+Connect+Configurations)
   to which they refer. There is an [experimental secret management](../../enterprise/kubernetes-guides/secret-management) guide
   to externalize secrets via Kubernetes.**

   ```
   curl -i -X \
      POST -H "Accept:application/json" -H "Content-Type:application/json" \
      [KAFKA CONNECT HOSTNAME:PORT]/connectors/ -d \
      '{ "name": "my-connector",
          "config": {
              "connector.class": "io.debezium.connector.mysql.MySqlConnector",
              "tasks.max": "1",
              "database.hostname": "[MYSQL HOST NAME]",
              "database.port": "[MYSQL PORT]",
              "database.user": "[DEBEZIUM USERNAME]",
              "database.password": "[DEBEZIUM PASSWORD]",
              "database.server.id": "[UNIQUE DATABASE SERVER ID]",
              "database.server.name": "[UNIQUE DATABASE SERVER NAME]",
              "database.include.list": "[DATABASES TO CONNECT]",
              "database.history.kafka.bootstrap.servers": "[KAFKA HOSTNAME:PORT]",
              "topic.prefix": "[DATABASE SERVER NAME]",
              "schema.history.internal.kafka.topic": "schema-changes.[DATABASE SERVER NAME].internal",
              "schema.history.internal.kafka.bootstrap.servers": "[KAFKA HOSTNAME:PORT]",
              "include.schema.changes": "true",
              "decimal.handling.mode": "string",
          }
      }'
    ```


## Step 4: Feldera input connector

Finally, you must create an input connector for each table in the SQL
table declaration.  For Feldera a Debezium connector uses the Kafka
transport, so the configuration is similar to the one for [Kafka input
connectors](kafka.md).


```sql
CREATE TABLE INPUT (
   ... -- columns omitted
) WITH (
  'connectors' = '[
    {
      "transport": {
          "name": "kafka_input",
          "config": {
              "bootstrap.servers": "KAFKA HOSTNAME:PORT",
              "auto.offset.reset": "earliest",
              "topics": ["UNIQUE DATABASE SERVER NAME.DATABASE.TABLE"]
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "debezium",
              "json_flavor": "debezium_mysql"
          }
      }
  }]'
)
```

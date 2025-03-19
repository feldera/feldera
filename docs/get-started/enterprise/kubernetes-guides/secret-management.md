# Secret management

:::caution Under Construction

This draft is for demo purposes with test setups.
Secret management is under ongoing development.
Be sure to take the [important configuration steps](https://kubernetes.io/docs/concepts/configuration/secret/) and follow the [good practices](https://kubernetes.io/docs/concepts/security/secrets-good-practices/) defined in the Kubernetes documentation to safely use secrets.

:::

:::caution Experimental feature

Secret management is an experimental feature of Feldera. Setting up secrets
currently involves a number of manual steps, which will be integrated
into an automated/improved workflow in the production release.

:::

Within certain connector configurations, it is possible to refer to secrets rather than inputting them as plain text. This prevents them from being visible in the connector configurations. We currently make use of [Kubernetes secrets](https://kubernetes.io/docs/concepts/configuration/secret/) for management.


## Feldera connector secrets

Referring to secrets is currently enabled only for the Feldera Kafka input and output connector configuration.

1. **Create kubernetes secret:** create a secret named `example` within the `feldera` namespace, which has a single key `example` with as value `something` (base64 encoded: `c29tZXRoaW5n`).
   We can do this by creating a file `example-secret.yaml`:
   ```
   apiVersion: v1
   kind: Secret
   metadata:
     name: example
     namespace: feldera
   type: Opaque
   data:
     example: c29tZXRoaW5n
   ```

   ... and subsequently apply it using: `kubectl apply -n feldera -f example-secret.yaml`

2. **Use the secret:** now, when we start the pipeline with a connector configuration value of `${secret:example}`, it will be resolved using a mounted secret volume containing the secret we just defined.


## Kafka Connect (Debezium) secrets

When creating a Kafka Connect Debezium connector to for example MySQL, it is required to provide database credentials. We make use of the Kafka Connect [FileConfigProvider](https://docs.confluent.io/platform/current/connect/security.html#fileconfigprovider), which allows us to refer to secrets in a similar way as Feldera connector secrets (with a slightly different pattern).

Because Kafka Connect is already running, it is not possible to mount new secrets without restarting. Instead, we patch an already existing mounted secret and await for it to be synchronized with the running instance.

**The steps are as follows:**

1. The secret used is `kafka-connect-databases-secret` in the `kafka-connect` namespace.
   It is mounted at `/etc/secret-volume-kafka-connect-databases` on the Kafka Connect instance.
   Check its presence by running:
   ```
   kubectl get secret -n kafka-connect kafka-connect-databases-secret
   ```

2. Suppose we want to store the MySQL database credentials.
   We need to get these credentials in a base64-encoded string.
   To do so, create a file `temp.properties` with the following content:
   ```
   hostname=mysql.some.domain.example.com
   port=3306
   user=chosen_debezium_user_123456
   password=chosen_debezium_password_123456
   ```

   ... and then we can base64 encode its content using:
   ```
   cat temp.properties | base64
   ```

   ... which will yield the following output:
   ```
   aG9zdG5hbWU9bXlzcWwuc29tZS5kb21haW4uZXhhbXBsZS5jb20KcG9ydD0zMzA2CnVzZXI9Y2hvc2VuX2RlYmV6aXVtX3VzZXJfMTIzNDU2CnBhc3N3b3JkPWNob3Nlbl9kZWJleml1bV9wYXNzd29yZF8xMjM0NTYK
   ```

3. Patch it into the secret:
   ```
   kubectl patch secret kafka-connect-databases-secret \
   -n kafka-connect \
   -p '{"data": { ".database-123456.properties": "aG9zdG5hbWU9bXlzcWwuc29tZS5kb21haW4uZXhhbXBsZS5jb20KcG9ydD0zMzA2CnVzZXI9Y2hvc2VuX2RlYmV6aXVtX3VzZXJfMTIzNDU2CnBhc3N3b3JkPWNob3Nlbl9kZWJleml1bV9wYXNzd29yZF8xMjM0NTYK"}}'
   ```

4. Check it is added by running:
   ```
   kubectl get secret -n kafka-connect kafka-connect-databases-secret -o yaml
   ```

5. It will take [some time](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod) before the patch is propagated to the mounted secret volume.

6. Within the connector curl call, define the following to refer to the secret:
   ```
   "database.hostname": "${file:/etc/secret-volume-kafka-connect-databases/.database-123456.properties:hostname}",
   "database.port": "${file:/etc/secret-volume-kafka-connect-databases/.database-123456.properties:port}",
   "database.user": "${file:/etc/secret-volume-kafka-connect-databases/.database-123456.properties:user}",
   "database.password": "${file:/etc/secret-volume-kafka-connect-databases/.database-123456.properties:password}",
   ```

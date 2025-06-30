# Development workflow

## Develop from Devcontainer

A simple way to start interacting with the Feldera source code is via the supplied Devcontainer. It will prepare an environment which will let you build and run all components of Feldera out-of-the-box.

Spin up the Devcontainer, e.g. by opening the Feldera repository in VS Code with [Dev Containers](vscode:extension/ms-vscode-remote.remote-containers) extension enabled.

On your first launch you might need to change the ownership of the files:
```bash
sudo chown -R user /workspaces/feldera
```

Build the SQL Compiler:

```bash
cd ./sql-to-dbsp-compiler && ./build.sh && cd ..
```

Build and start the Pipeline Manager (that also serves the Feldera Web Console):

```bash
RUST_LOG=info RUST_BACKTRACE=1 cargo run --bin pipeline-manager -- --db-connection-string postgres-embed --dev-mode --bind-address 0.0.0.0 --dbsp-override-path .  --compiler-working-directory ~/.feldera --runner-working-directory ~/.feldera
```

> Here, `~/.feldera` is the directory that will host compilation artifacts of SQL Compiler and other state of the Pipeline Manager and the pipelines. It will be created if it doesn't exist, and you can use a different directory.

> `--dbsp-override-path .` should be the path of the Feldera repository root - so update the argument if you are running from a different directory.

You should now be able to access the Web Console at http://127.0.0.1:8080/, connected to your local Pipeline Manager instance!

You can also open Web Console in dev mode to be able to see your changes to it live:

```bash
cd web-console && bun install && bun run dev
```

The Web Console in dev mode is available at http://127.0.0.1:3000/

Now you can proceed with the [demo](#manually-starting-the-demos).

#### Authenticated mode
In Authenticated mode, you need to login via the Web Console using one of the supported OAuth providers (e.g. AWS Cognito). The Pipeline Manager will require Bearer authorization header for protected requests.

Start the Pipeline Manager in authenticated mode, substituting values from your environment:
```bash
AUTH_CLIENT_ID=<client-id> AUTH_ISSUER=<issuer> <see below for additional environment variables> \
 cargo run --bin pipeline-manager -- --auth-provider=aws-cognito
```

##### AWS Cognito

First, setup an AWS Cognito user pool, configure an app client and enable the
[Amplify-based hosted
UI](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-app-integration.html#cognito-user-pools-app-integration-amplify).
When setting up the hosted UI, setup a redirect URI as
`<feldera-api-url>/auth/aws/` (note the trailing slash). For example, when
running Feldera on 127.0.0.1:8080, the redirect URI should be
`http://127.0.0.1:8080/auth/aws/`. In this process, you will also set up a
`Cognito domain`, which will be something like
`https://<domain-name>.auth.us-east-1.amazoncognito.com`. This domain forms the
base of the [login and logout
URL](https://docs.aws.amazon.com/cognito/latest/developerguide/login-endpoint.html)
which you will need below.

Additional variables for AWS Cognito:
- AWS_COGNITO_LOGIN_URL: URL to Cognito Hosted UI login, omitting query parameters `redirect_uri` and `state`
- AWS_COGNITO_LOGOUT_URL: URL to Cognito Hosted UI logout, omitting query parameters `redirect_uri` and `state`

Example:
```bash
AUTH_CLIENT_ID=xxxxxxxxxxxxxxxxxxxxxxxxxx AUTH_ISSUER=https://cognito-idp.us-east-1.amazonaws.com/us-east-1_xxxxxxxxx AWS_COGNITO_LOGIN_URL="https://itest-pool.auth.us-east-1.amazoncognito.com/login\?client_id=xxxxxxxxxxxxxxxxxxxxxxxxxx&response_type=token&scope=email+openid" AWS_COGNITO_LOGOUT_URL="https://itest-pool.auth.us-east-1.amazoncognito.com/logout\?client_id=xxxxxxxxxxxxxxxxxxxxxxxxxx&response_type=token&scope=email+openid" RUST_LOG=debug,tokio_postgres=info cargo run --bin=pipeline-manager -- --dev-mode --auth-provider aws-cognito
```

##### Google Identity Platform
Additional variables for Google Identity Platform: none

Example:
```bash
AUTH_CLIENT_ID=xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.apps.googleusercontent.com AUTH_ISSUER="https://accounts.google.com" RUST_LOG=debug,tokio_postgres=info cargo run --bin=pipeline-manager -- --dev-mode --auth-provider google-identity
```

## Run benchmarks

You can run bundled benchmarks in devcontainer.

#### NEXMark benchmark
Example running one of nexmark queries:
```bash
cd benchmark
./run-nexmark.sh -L sql --query 9 --partitions 1 --events 10M --kafka-broker redpanda:9092
```

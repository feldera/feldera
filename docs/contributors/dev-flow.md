# Development workflow

## Develop from Devcontainer

A simple way to start interacting with the Feldera source code is via the supplied Devcontainer. It will prepare an environment which will let you build and run all components of Feldera out-of-the-box.

Spin up the Devcontainer, e.g. by opening the Feldera repository in VS Code with [Dev Containers](vscode:extension/ms-vscode-remote.remote-containers) extension enabled.

On your first launch you might need to change the ownership of the files:
```bash
sudo chown -R user /workspaces/dbsp
```

Build the SQL Compiler:

```bash
mvn -f ./sql-to-dbsp-compiler/SQL-compiler -DskipTests package
```

Build and start the Pipeline Manager (that also serves the Feldera Web Console):

```bash
RUST_LOG=info RUST_BACKTRACE=1 cargo run --bin pipeline-manager --features pg-embed -- --api-server-working-directory ~/.dbsp -d postgres-embed --dev-mode --bind-address 0.0.0.0 --sql-compiler-home ./sql-to-dbsp-compiler --dbsp-override-path .  --compiler-working-directory ~/.dbsp --runner-working-directory ~/.dbsp
```
RUST_LOG=info RUST_BACKTRACE=1 AUTH_CLIENT_ID=44jttbufih62p9hh999f3irjkm AUTH_ISSUER="https://cognito-idp.us-east-1.amazonaws.com/us-east-1_gkyurqf2T" AWS_COGNITO_REGION="us-east-1" AWS_COGNITO_USER_POOL_ID="us-east-1_gkyurqf2T" cargo run --bin pipeline-manager --features pg-embed -- --api-server-working-directory ~/.dbsp -d postgres-embed --dev-mode --bind-address 0.0.0.0 --sql-compiler-home ./sql-to-dbsp-compiler --dbsp-override-path .  --compiler-working-directory ~/.dbsp --runner-working-directory ~/.dbsp --auth-provider=aws-cognito

> Here, `~/.dbsp` is the directory that will host compilation artifacts of SQL Compiler that Pipeline Manager will then use. It will be created if it doesn't exist, and you can use another name for it - just replace corresponding arguments in the above command.

> `--dbsp-override-path .` should be the path of the Feldera repository root - so update the argument if you are running from a different directory.

You should now be able to access the Web Console at http://localhost:8080/, connected to your local Pipeline Manager instance!

You can also open Web Console in dev mode to be able to see your changes to it live:

```bash
cd web-console && yarn install && yarn dev
```

The Web Console in dev mode is available at http://localhost:3000/

Now you can proceed with the [demo](#manually-starting-the-demos).

#### Authenticated mode
In Authenticated mode, you need to login via the Web Console using one of the supported OAuth providers (e.g. AWS Cognito). The Pipeline Manager will require Bearer authorization header for protected requests.

Start the Pipeline Manager in authenticated mode, substituting values from your environment:
```bash
AUTH_CLIENT_ID=<client-id> AUTH_ISSUER=<issuer> <see below for additional environment variables> \ 
 cargo run --bin pipeline-manager --features pg-embed -- --auth-provider=aws-cognito
```

##### AWS Cognito

First, setup an AWS Cognito user pool, configure an app client and enable the
[Amplify-based hosted
UI](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-app-integration.html#cognito-user-pools-app-integration-amplify).
When setting up the hosted UI, setup a redirect URI as
`<feldera-api-url>/auth/aws/` (note the trailing slash). For example, when
running Feldera on localhost:8080, the redirect URI should be
`http://localhost:8080/auth/aws/`. In this process, you will also set up a
`Cognito domain`, which will be something like
`https://<domain-name>.auth.us-east-1.amazoncognito.com`. This domain forms the
base of the [login and logout
URL](https://docs.aws.amazon.com/cognito/latest/developerguide/login-endpoint.html)
which you will need below.

Additional variables for AWS Cognito:
- AWS_COGNITO_LOGIN_URL: URL to Cognito Hosted UI login, omitting query parameters `redirect_uri` and `state`
- AWS_COGNITO_LOGOUT_URL: URL to Cognito Hosted UI logout, omitting query parameters `redirect_uri` and `state`

##### Google Identity Platform
Additional variables for Google Identity Platform: none

## Develop on your machine

TODO

## Launch the prepared demo

Refer to the [Get Started page](/docs/intro) for basic instructions on spinning up and interacting with the demos from a separate docker-compose.

## Manually starting the demos

You can prepare and run multiple demos. When initializing any demo, Pipeline Manager needs to be running. Preparing the demos might take more than 10 minutes.

#### All demos

To prepare all available demos, run
```bash
DBSP_MANAGER="http://localhost:8080" demo/create_demo_projects.sh && DBSP_MANAGER="http://localhost:8080" demo/prepare_demo_data.sh
```

#### Single demo
To prepare a single demo navigate to the directory of the demo, e.g.
```bash
cd demo/project_demo00-SecOps
```

If the `simulator` directory exists - the following command generates simulated input data and creates the required Kafka input and output topics:
```bash
cargo run --manifest-path simulator/Cargo.toml --release -- 300000
```

> For SecOps demo, argument `300000` specifies the number of simulated CI pipelines to be generated, which impacts duration of the demo and system load. YMMV!

Use `dbsp` python library to create and compile SQL Program, and prepare the Pipeline that utilizes them.
```bash
python3 run.py --dbsp_url http://localhost:8080 --actions prepare
```

> You should update `--dbsp_url` depending on where Pipeline Manager is getting served from.

<br/>

Now you can see the prepared demos on the Pipeline Management page of the Web Console.

# Add Feldera domains to your network

To use Feldera in a network that requires you to allowlist new domains, ask your network administrator to allowlist the following hostnames (all over `https`, port `443`).

## Standard Usage
- `https://cloud1.feldera.com`: License key validation and usage-telemetry server. (**Enterprise Only**)
- `https://public.ecr.aws/feldera/*`: The Feldera Enterprise Helm Chart pulls images from this repository. (**Enterprise Only**)

## Custom Runtime Usage (Optional)
In some cases, advanced users deploy a custom Feldera runtime to test features before they are officially released. To use a custom runtime, enable the `runtime_version` flag via the [`unstableFeatures` Helm value](/get-started/enterprise/helm-chart-reference#miscellaneous).
- `static.crates.io` and `index.crates.io`: If the runtime version has a new dependency, the crate and its index are downloaded from here.
- `https://github.com/feldera/feldera`: Feldera source code.
- `https://feldera-sql2dbsp.s3.us-west-1.amazonaws.com/*`: Build artifacts for the custom runtime, published by Feldera CI/CD.

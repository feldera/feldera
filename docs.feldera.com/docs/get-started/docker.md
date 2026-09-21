# Docker

These instructions explain how to run Feldera on a single machine
in a configuration suitable for demos, development, and testing.  For production
use, check out [Feldera Enterprise](/get-started/enterprise).

## Docker Quickstart

```
docker run --pull always -p 8080:8080 --tty --rm -it images.feldera.com/feldera/pipeline-manager:latest
```

Once you see the Feldera logo on your terminal, go ahead and open the Web Console
at `http://127.0.0.1:8080` and try out one of our pre-packaged demo pipelines.

## Optional: Docker Compose Quickstart

We also make a Docker Compose file available. It's useful if you want to use
Feldera with auxiliary services included in the Docker Compose file
like Redpanda, Prometheus and Grafana.

```
curl -L 'https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml' | \
docker compose -f - up
```

You can enable specific services from the Docker Compose file as follows:

```
curl -L 'https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml' | \
docker compose -f - up pipeline-manager redpanda
```

Similar to the previous section, once you see the Feldera logo on your
terminal, go ahead and open the Web Console at `http://127.0.0.1:8080` and try
out one of our pre-packaged demo pipelines.

## Serving Feldera under a URL subpath

By default the web console and the REST API are served from the root of the
origin: `http://127.0.0.1:8080/` and `http://127.0.0.1:8080/v0`. Set
`FELDERA_HTTP_BASE_PATH` (or the `--http-base-path` flag) when a reverse proxy
or ingress mounts Feldera on a subpath instead, such as
`https://example.com/feldera/`:

```
docker run -p 8080:8080 -e FELDERA_HTTP_BASE_PATH=/feldera \
  images.feldera.com/feldera/pipeline-manager:latest
```

The value must start with `/` and must not end with one; the default empty
value serves from the root. The console then lives at `<base path>/`, the API
at `<base path>/v0`, and the unauthenticated config API at `<base path>/config`.
The manager rewrites the embedded console bundle at startup, so one image
serves any subpath and no rebuild is needed.

The proxy must forward the prefix rather than strip it, and must pass WebSocket
upgrades through, because pipeline logs and performance charts stream over
WebSocket. With nginx, write `proxy_pass` without a URI part:

```nginx
location /feldera/ {
    # No path after the host: nginx forwards the original URI, prefix included.
    proxy_pass http://feldera:8080;
    proxy_set_header Host $host;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}
```

Caddy keeps the prefix with `handle` (`handle_path` would strip it) and
upgrades WebSockets on its own:

```
example.com {
    handle /feldera/* {
        reverse_proxy feldera:8080
    }
}
```

Point API clients at the origin plus the prefix, for example `fda --host
https://example.com/feldera pipelines`. Each client appends `/v0` itself.

### Health checks and robots.txt

With a base path set, `/healthz` and `/robots.txt` are available both:
at the origin root, where a probe that reaches the container directly
looks (a Kubernetes liveness probe, or a load balancer targeting the pod), and
under the prefix, which is the only path a proxy forwarding `<base path>/*`
can reach. Point a proxied health check at `<base path>/healthz`; no extra
proxy rule is needed.

## Installing Docker

If you don't already have Docker or Docker Compose installed, follow one of these steps first:

* On Mac OS, Windows, or Linux, install [Docker Desktop][1].
  If you're on Apple Silicon,
  we recommend [enabling Rosetta](https://docs.docker.com/desktop/settings/mac/#use-rosetta-for-x86amd64-emulation-on-apple-silicon)
  for x86/amd64 emulation.

* On Linux only, first install [Docker Engine][2] and the [Docker
  Compose plugin][3].

  :::tip

  The plugin implements Docker Compose v2, invoked as `docker
  compose`. Feldera does not support the older Docker Compose v1,
  which was invoked with `docker-compose` (note the ` ` versus `-`
  distinction).

  :::

  Then, follow the instructions to [manage Docker as a non-root
  user][4].

  :::tip

  If you only want root to manage Docker, you can prefix
  the `docker compose` command below with `sudo`.

  :::

You also need `curl` and a web browser such as Chrome or Firefox.

[1]: https://docs.docker.com/desktop/
[2]: https://docs.docker.com/engine/install/
[3]: https://docs.docker.com/compose/install/linux
[4]: https://docs.docker.com/engine/install/linux-postinstall/


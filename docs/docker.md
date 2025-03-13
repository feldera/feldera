# Docker

These instructions explain how to run Feldera on a single machine
in a configuration suitable for demos, development, and testing.  For production
use, check out [Feldera Enterprise](/enterprise).

## Docker Quickstart

```
docker run -p 8080:8080 --tty --rm -it ghcr.io/feldera/pipeline-manager:0.39.0
```

Once you see the Feldera logo on your terminal, go ahead and open the Web Console
at `http://localhost:8080` and try out one of our pre-packaged demo pipelines.

## Optional: Docker Compose Quickstart

We also make a Docker Compose file available. It's useful if you want to use
Feldera with auxiliary services included in the Docker Compose file
like Redpanda, Prometheus and Grafana.

```
curl -L https://github.com/feldera/feldera/releases/latest/download/docker-compose.yml | \
docker compose -f - up
```

You can enable specific services from the Docker Compose file as follows:

```
curl -L https://github.com/feldera/feldera/releases/latest/download/docker-compose.yml | \
docker compose -f - up pipeline-manager redpanda
```

Similar to the previous section, once you see the Feldera logo on your
terminal, go ahead and open the Web Console at `http://localhost:8080` and try
out one of our pre-packaged demo pipelines.

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


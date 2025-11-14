# Feldera I/O adapter framework

This crate implements an infrastructure to ingest data into a DBSP
circuit from external data sources and to stream the outputs of the
circuit to external consumers. It also implements a Feldera I/O
controller that controls the execution of a DBSP circuit along with
its input and output adapters, and a server that exposes the
controller API over HTTP and through a web interface.

## Dependencies

The test code has the following dependencies:

- `cmake`:
  > $ sudo apt install cmake

- `redpanda`:

  On Debian or Ubuntu:

  ```sh
  curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash
  sudo apt install redpanda -y
  sudo systemctl start redpanda
  ```

  Or with Docker:

  ```sh
  docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v24.2.4 redpanda start --smp 2
  ```

- `NATS`:

  The tests for the NATS input connector expect the binary `nats-server` to be available.

  To install on Debian or Ubuntu:

  ```sh
  sudo apt install nats-server -y
  ```

## DBSP application server demo

This directory also contains a demo application runnign a very simple
DBSP pipeline as a service. The service can be controlled using a web
browser. To run the demo you can execute the following command:

```
$ cargo run --example server --features="with-kafka server"
```

Then open a web browser and open the following URL: `http://localhost:8080`

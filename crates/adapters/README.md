# DBSP I/O adapter framework

This crate implements an infrastructure to ingest data into a DBSP
circuit from external data sources and to stream the outputs of the
circuit to external consumers. It also implements a DBSP I/O
controller that controls the execution of a DBSP circuit along with
its input and output adapters, and a DBSP server that exposes the
controller API over HTTP and through a web interface.

## Dependencies

The test code has the following dependencies:

- `cmake`:
  >$ sudo apt install cmake

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

Furthermore, for developing client (web browser) UI used to monitor a
DBSP application server, the following tools need to be installed:

- `npm` (node package manager):
  - You can verify if `npm` is installed using
  >$ npm version
  - If not installed, go to <https://nodejs.org/en/download> the node.js downloads page.
  - Select any of the available versions of Node.js to download and install it.
  - To verify that `npm` is installed, open the Command Prompt window, and then enter
  >$ npm version.

- `typescript`:
  - You can verify if typescript is already available by typing `tsc`.
  If not, you can install it globally using
  >$ npm install -g typescript

To generate the web browser UI run the following commands:

```
$ cd static
$ tsc
```

## DBSP application server demo

This directory also contains a demo application runnign a very simple
DBSP pipeline as a service.  The service can be controlled using a web
browser.  To run the demo you can execute the following command:

```
$ cargo run --example server --features="with-kafka server"
```

Then open a web browser and open the following URL: `http://localhost:8080`

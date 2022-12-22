# DBSP application server demo

This directory contains a demo application runnign a DBSP pipeline as a service.
The service can be controlled from a web server.

## Dependencies

You need to install the following software:

- `cmake`:
  >$ sudo apt install cmake

- `redpanda`:
  
  ```sh
  curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash
  sudo apt install redpanda -y 
  sudo systemctl start redpanda
  ```
  
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

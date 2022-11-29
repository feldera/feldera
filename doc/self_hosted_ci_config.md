# CI self-hosted runners

Steps on how to set-up and configure a Ubuntu 22.04 Linux self-hosted CI runner
machine.

## Configuration

```bash
sudo apt-get install libssl-dev zsh build-essential pkg-config git gcc clang libclang-dev python3-pip hub numactl cmake
sudo useradd github-runner -m -s /bin/zsh
```

In `$HOME/.zshrc` add python binaries folder to the PATH:

```bash
export PATH=/home/github-runner/.local/bin:$PATH
```


Add sudo capability for github-runner:

```bash
sudo visudo
# github-runner  ALL=(ALL) NOPASSWD: ALL
```

Login and install rust:

```bash
sudo su github-runner
cd $HOME
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Other than that, follow the steps listed under Settings -> Actions -> Runner -> Add runner:

```
<< steps from Web-UI >>
```

Ensure the runners have permission to access the benchmarks repo and the source
code repo in case you want to do fast-forward merging.

## Starting the runner

Finally, launch the runner:

```bash
cd $HOME/actions-runner
source $HOME/.cargo/env
./run.sh
```

## Starting the runner as systemd service

```bash
cd $HOME/actions-runner
sudo ./svc.sh install
sudo ./svc.sh start
```

Check the runner status with:

```bash
sudo ./svc.sh status
```

## Stopping the runner

```bash
sudo ./svc.sh stop
```

Uninstall the service with:

```bash
sudo ./svc.sh uninstall
```

## update and install some things we should probably have
apt-get update
apt-get install -y \
  curl \
  git \
  gnupg2 \
  sudo \
  build-essential \
  openssl \
  cmake \
  pkg-config \
  libssl-dev

## Install rustup and common components
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup install nightly
rustup component add rustfmt
rustup component add rustfmt --toolchain nightly
rustup component add clippy
rustup component add clippy --toolchain nightly

## Install nodejs and npm packages
curl -fsSL https://deb.nodesource.com/setup_19.x | bash -
apt-get install -y nodejs
npm install --global yarn
npm install --global openapi-typescript-codegen

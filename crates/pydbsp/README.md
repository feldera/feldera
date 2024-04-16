# Instructions

## Install

```
cd crates/pydbsp
python3 -m venv .env

source .env/bin/activate
pip3 install maturin
pip3 install "sqlglot[rs]"
pip3 install patchelf
```

## Run

```
source .env/bin/activate
maturin develop
python3 test.py
```

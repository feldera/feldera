#!/bin/bash

set -ex

../SQL-compiler/sql-to-dbsp -i --ignoreOrder -v 4 -o src/final.png -png primary.sql
../SQL-compiler/sql-to-dbsp -i --ignoreOrder -o src/lib.rs primary.sql
RUST_BACKTRACE=1 cargo run --release

#! /bin/sh
set -ex
test -d beam || git clone https://github.com/apache/beam.git
(cd beam && git checkout origin/release-2.55.0)
(cd beam && git am < ../configurable-spark-master.patch)

#! /bin/sh
set -ex
test -d beam || git clone https://github.com/apache/beam.git
(cd beam && git checkout v2.46.0)
(cd beam && git am < ../configurable-spark-master.patch)

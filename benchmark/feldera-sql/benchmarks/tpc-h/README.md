# TPC-H

These instructions allow you to run TPC-H at scale 100 (with about 100
GiB of CSV data) in Postgres and Feldera, dividing the data into 10
equal-size chunks.

## Data generation

First, generate the data:

```
git clone git@github.com:gregrahn/tpch-kit.git
cd tpch-kit
export DSS_CONFIG=$PWD/dbgen
export DSS_QUERY=$DSS_CONFIG/queries
export DSS_PATH=$PWD/output
mkdir output
cd dbgen
make
./dbgen -s100           # This will take several minutes
cd ../output
for file in *.tbl; do mv $file $(echo "$file" | sed 's/tbl/csv/'); done
```

Then move the `.csv` files into the `s100` directory under this
subdirectory.  Then break `orders.csv` and `lineitem.csv` into 10
equal chunks:

```
cd s100
mv orders.csv orders-full.csv
mv lineitem.csv lineitem-full.csv
./shard.py              # This will take a minute or two.
```

## Running with Postgres

The `run-postgres.sh` script in the s100 directory will run under
Postgres:

```
cd s100
sudo -u postgres ./run-postgres.sh
```

* On Fedora, I had to run as the `postgres` user (see the `sudo`
  above).  There must be some way to avoid that but it was easy.

* I kept getting "permission denied" errors for trying to read the
  generated data until some Googling found that the problem was
  related to security labels.  I fixed it with enough applications of
  commands like this:

  ```
  sudo chcon --reference /var/lib/pgsql/data /home/blp/feldera/dbsp/benchmark/feldera-sql/benchmarks/tpc-h/s100/*
  ```

## Running with Feldera

From the `feldera-sql` directory three levels up, run a command like this:

```
./run.py --folder benchmarks/tpc-h --api-url=http://localhost:8080 --query q5 --storage
```

The output is a quick hack.  There are 11 lines, like this:

```
Started pipeline tpc-h-q5 in 5.6 s
Pipeline tpc-h-q5 processed 116000030 records in 106.0 seconds (1.9 GiB peak memory, 1155.0 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 190986082 records in 168.4 seconds (4.8 GiB peak memory, 4054.4 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 265994638 records in 171.5 seconds (5.1 GiB peak memory, 7001.8 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 340998402 records in 172.6 seconds (5.4 GiB peak memory, 9944.1 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 416012320 records in 173.7 seconds (5.4 GiB peak memory, 12771.2 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 491005841 records in 172.7 seconds (5.4 GiB peak memory, 15542.2 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 566011624 records in 173.7 seconds (6.1 GiB peak memory, 18441.8 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 641017468 records in 172.7 seconds (6.1 GiB peak memory, 21083.8 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 716025159 records in 173.4 seconds (6.4 GiB peak memory, 23892.8 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 791030462 records in 175.7 seconds (6.4 GiB peak memory, 26733.0 s CPU time, 0 late drops)
Pipeline tpc-h-q5 processed 866037932 records in 173.5 seconds (6.4 GiB peak memory, 29404.0 s CPU time, 0 late drops)
```

The first line is the time to load the data not in the lineitem or
orders table.  The subsequent 10 lines are one chunk each.  The times
are per-chunk, the other statistics are cumulative.

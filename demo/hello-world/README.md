# Demo: hello-world

## Getting started

1. Bring up a Feldera instance, for example reachable at `http://localhost:8080`.
   Note that this demo requires the script and the Feldera instance to be running
   on the same machine as files are directly used for input and output.

2. Run the following:
   ```
   cd demo/hello-world
   python3 run.py --api-url http://localhost:8080
   ```

   This will run the pipeline, inputting `messages.csv` and `records.csv`, combining
   them as defined in the SQL program `combiner.sql`, and outputting `matches.csv`.

3. Once the pipeline is run, you should see an output file named `matches.csv`
   be created in this directory with the message `Hello world!,1` (1 indicating
   that this tuple was added to the output collection).

## Usage

```
python3 run.py --help
```
